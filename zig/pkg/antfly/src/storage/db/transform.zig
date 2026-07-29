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

pub fn resolveDocumentTransform(
    alloc: Allocator,
    existing_json: ?[]const u8,
    transform: types.DocumentTransform,
) !?[]u8 {
    try validateDocumentTransform(transform);
    if (existing_json == null and !transform.upsert) return null;
    const is_insert = existing_json == null;

    var root = if (existing_json) |body| blk: {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
        defer parsed.deinit();
        if (parsed.value != .object) return error.InvalidArgument;
        break :blk try cloneJsonValue(alloc, parsed.value);
    } else std.json.Value{ .object = std.json.ObjectMap.empty };
    defer freeJsonValue(alloc, &root);

    for (transform.operations) |op| try applyTransformOp(alloc, &root, op, is_insert);

    return try std.json.Stringify.valueAlloc(alloc, root, .{});
}

/// Validates the complete transform at admission and storage boundaries.
///
/// Keep this independent of document state so an unsupported request cannot
/// become an acknowledged no-op merely because its target is absent.
pub fn validateDocumentTransform(transform: types.DocumentTransform) !void {
    for (transform.operations) |op| try validateTransformOpType(op.op);
}

pub fn validateTransformOpType(op: types.TransformOpType) !void {
    switch (op) {
        .set, .set_on_insert, .unset, .inc, .add_to_set, .max => {},
        else => return error.UnsupportedTransformOperation,
    }
}

pub fn transformOpText(op: types.TransformOpType) []const u8 {
    return switch (op) {
        .set => "$set",
        .set_on_insert => "$setOnInsert",
        .unset => "$unset",
        .inc => "$inc",
        .push => "$push",
        .pull => "$pull",
        .add_to_set => "$addToSet",
        .pop => "$pop",
        .mul => "$mul",
        .min => "$min",
        .max => "$max",
        .current_date => "$currentDate",
        .rename => "$rename",
    };
}

fn applyTransformOp(alloc: Allocator, root: *std.json.Value, op: types.TransformOp, is_insert: bool) !void {
    if (root.* != .object) return error.InvalidArgument;
    switch (op.op) {
        .set => try setOp(alloc, &root.object, op.path, op.value_json),
        .set_on_insert => if (is_insert) try setOp(alloc, &root.object, op.path, op.value_json),
        .unset => try unsetOp(alloc, &root.object, op.path),
        .inc => try incOp(alloc, &root.object, op.path, op.value_json orelse return error.InvalidArgument),
        .add_to_set => try addToSetOp(alloc, &root.object, op.path, op.value_json orelse return error.InvalidArgument),
        .max => try maxOp(alloc, &root.object, op.path, op.value_json orelse return error.InvalidArgument),
        else => return error.UnsupportedTransformOperation,
    }
}

const NormalizedJsonPath = struct {
    parts: [32][]const u8 = undefined,
    len: usize = 0,

    fn slice(self: *const NormalizedJsonPath) []const []const u8 {
        return self.parts[0..self.len];
    }
};

fn normalizeJsonPath(path: []const u8) !NormalizedJsonPath {
    var normalized = path;
    if (normalized.len > 0 and normalized[0] == '$') {
        if (normalized.len == 1) return .{};
        if (normalized.len < 2 or normalized[1] != '.') return error.InvalidArgument;
        normalized = normalized[2..];
    }
    if (normalized.len == 0) return .{};
    var count: usize = 1;
    for (normalized) |ch| {
        if (ch == '.') count += 1;
    }
    if (count > 32) return error.InvalidArgument;
    var parts = NormalizedJsonPath{};
    var it = std.mem.splitScalar(u8, normalized, '.');
    while (it.next()) |part| {
        if (part.len == 0) return error.InvalidArgument;
        parts.parts[parts.len] = part;
        parts.len += 1;
    }
    if (parts.len != count) return error.InvalidArgument;
    return parts;
}

fn setOp(
    alloc: Allocator,
    obj: *std.json.ObjectMap,
    path: []const u8,
    value_json: ?[]const u8,
) !void {
    const normalized = try normalizeJsonPath(path);
    var value: std.json.Value = if (value_json) |json| blk: {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
        defer parsed.deinit();
        break :blk try cloneJsonValue(alloc, parsed.value);
    } else .null;
    errdefer freeJsonValue(alloc, &value);
    try setNestedValue(alloc, obj, normalized.slice(), value);
}

fn unsetOp(alloc: Allocator, obj: *std.json.ObjectMap, path: []const u8) !void {
    const normalized = try normalizeJsonPath(path);
    removeNestedValue(alloc, obj, normalized.slice());
}

fn incOp(
    alloc: Allocator,
    obj: *std.json.ObjectMap,
    path: []const u8,
    value_json: []const u8,
) !void {
    const normalized = try normalizeJsonPath(path);
    const parts = normalized.slice();
    const delta = try parseNumericValue(alloc, value_json);
    if (parts.len == 0) return error.InvalidArgument;
    if (getNestedValue(obj, parts)) |current| {
        const current_num = try jsonNumberFromValue(current.*);
        try setNestedValue(alloc, obj, parts, .{ .float = current_num + delta });
        return;
    }
    try setNestedValue(alloc, obj, parts, .{ .float = delta });
}

fn maxOp(
    alloc: Allocator,
    obj: *std.json.ObjectMap,
    path: []const u8,
    value_json: []const u8,
) !void {
    const normalized = try normalizeJsonPath(path);
    const parts = normalized.slice();
    const candidate = try parseNumericValue(alloc, value_json);
    if (parts.len == 0) return error.InvalidArgument;
    if (getNestedValue(obj, parts)) |current| {
        const current_num = try jsonNumberFromValue(current.*);
        if (candidate <= current_num) return;
    }
    try setNestedValue(alloc, obj, parts, .{ .float = candidate });
}

fn addToSetOp(
    alloc: Allocator,
    obj: *std.json.ObjectMap,
    path: []const u8,
    value_json: []const u8,
) !void {
    const normalized = try normalizeJsonPath(path);
    const parts = normalized.slice();
    var value = blk: {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, value_json, .{});
        defer parsed.deinit();
        break :blk try cloneJsonValue(alloc, parsed.value);
    };
    errdefer freeJsonValue(alloc, &value);

    if (parts.len == 0) return error.InvalidArgument;
    if (getNestedValue(obj, parts)) |existing| {
        switch (existing.*) {
            .array => |*arr| {
                for (arr.items) |item| {
                    if (jsonValuesEqual(item, value)) return;
                }
                try arr.append(value);
                return;
            },
            else => return error.InvalidArgument,
        }
    }

    var arr = std.json.Array.init(alloc);
    errdefer {
        for (arr.items) |*item| freeJsonValue(alloc, item);
        arr.deinit();
    }
    try arr.append(value);
    try setNestedValue(alloc, obj, parts, .{ .array = arr });
}

fn getNestedValue(obj: *std.json.ObjectMap, parts: []const []const u8) ?*std.json.Value {
    if (parts.len == 0) return null;
    var current: *std.json.Value = obj.getPtr(parts[0]) orelse return null;
    var idx: usize = 1;
    while (idx < parts.len) : (idx += 1) {
        current = switch (current.*) {
            .object => |*nested| nested.getPtr(parts[idx]) orelse return null,
            else => return null,
        };
    }
    return current;
}

fn setNestedValue(
    alloc: Allocator,
    obj: *std.json.ObjectMap,
    parts: []const []const u8,
    value: std.json.Value,
) !void {
    if (parts.len == 0) return error.InvalidArgument;
    var current = obj;
    if (parts.len > 1) {
        var idx: usize = 0;
        while (idx < parts.len - 1) : (idx += 1) {
            const part = parts[idx];
            const gop = try current.getOrPut(alloc, part);
            if (!gop.found_existing) {
                gop.key_ptr.* = try alloc.dupe(u8, part);
                gop.value_ptr.* = .{ .object = std.json.ObjectMap.empty };
            } else if (gop.value_ptr.* != .object) {
                freeJsonValue(alloc, gop.value_ptr);
                gop.value_ptr.* = .{ .object = std.json.ObjectMap.empty };
            }
            current = &gop.value_ptr.object;
        }
    }
    const leaf = parts[parts.len - 1];
    if (current.getPtr(leaf)) |existing| {
        freeJsonValue(alloc, existing);
        existing.* = value;
        return;
    }
    try current.put(alloc, try alloc.dupe(u8, leaf), value);
}

fn removeNestedValue(alloc: Allocator, obj: *std.json.ObjectMap, parts: []const []const u8) void {
    if (parts.len == 0) return;
    if (parts.len == 1) {
        if (obj.fetchSwapRemove(parts[0])) |entry| {
            alloc.free(entry.key);
            var value = entry.value;
            freeJsonValue(alloc, &value);
        }
        return;
    }

    var current = obj;
    var idx: usize = 0;
    while (idx < parts.len - 1) : (idx += 1) {
        const next = current.getPtr(parts[idx]) orelse return;
        current = switch (next.*) {
            .object => |*nested| nested,
            else => return,
        };
    }
    if (current.fetchSwapRemove(parts[parts.len - 1])) |entry| {
        alloc.free(entry.key);
        var value = entry.value;
        freeJsonValue(alloc, &value);
    }
}

fn parseNumericValue(alloc: Allocator, value_json: []const u8) !f64 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, value_json, .{});
    defer parsed.deinit();
    return try jsonNumberFromValue(parsed.value);
}

fn jsonNumberFromValue(value: std.json.Value) !f64 {
    return switch (value) {
        .integer => |number| @floatFromInt(number),
        .float => |number| number,
        else => error.InvalidArgument,
    };
}

fn jsonValuesEqual(left: std.json.Value, right: std.json.Value) bool {
    return switch (left) {
        .null => right == .null,
        .bool => |v| right == .bool and right.bool == v,
        .integer => |v| right == .integer and right.integer == v,
        .float => |v| right == .float and right.float == v,
        .number_string => |v| right == .number_string and std.mem.eql(u8, right.number_string, v),
        .string => |v| right == .string and std.mem.eql(u8, right.string, v),
        .array => |arr| blk: {
            if (right != .array or arr.items.len != right.array.items.len) break :blk false;
            for (arr.items, right.array.items) |lhs, rhs| {
                if (!jsonValuesEqual(lhs, rhs)) break :blk false;
            }
            break :blk true;
        },
        .object => |obj| blk: {
            if (right != .object or obj.count() != right.object.count()) break :blk false;
            var it = obj.iterator();
            while (it.next()) |entry| {
                const other = right.object.get(entry.key_ptr.*) orelse break :blk false;
                if (!jsonValuesEqual(entry.value_ptr.*, other)) break :blk false;
            }
            break :blk true;
        },
    };
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

test "resolve document transform supports set setOnInsert max inc and addToSet" {
    const alloc = std.testing.allocator;

    const transform: types.DocumentTransform = .{
        .key = "doc:1",
        .upsert = false,
        .operations = &.{
            .{ .op = .set_on_insert, .path = "owner", .value_json = "\"system\"" },
            .{ .op = .max, .path = "version", .value_json = "10" },
            .{ .op = .set, .path = "status", .value_json = "\"updated\"" },
            .{ .op = .inc, .path = "views", .value_json = "2" },
            .{ .op = .add_to_set, .path = "tags", .value_json = "\"zig\"" },
        },
    };

    const resolved = try resolveDocumentTransform(
        alloc,
        "{\"version\":5,\"views\":1,\"tags\":[\"db\"]}",
        transform,
    );
    defer alloc.free(resolved.?);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, resolved.?, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(f64, 10), try jsonNumberFromValue(parsed.value.object.get("version").?));
    try std.testing.expectEqual(@as(f64, 3), try jsonNumberFromValue(parsed.value.object.get("views").?));
    try std.testing.expectEqualStrings("updated", parsed.value.object.get("status").?.string);
    try std.testing.expect(parsed.value.object.get("owner") == null);
    try std.testing.expectEqual(@as(usize, 2), parsed.value.object.get("tags").?.array.items.len);
}

test "resolve document transform applies setOnInsert only when upsert inserts" {
    const alloc = std.testing.allocator;
    const transform: types.DocumentTransform = .{
        .key = "doc:new",
        .upsert = true,
        .operations = &.{
            .{ .op = .set_on_insert, .path = "canonical_name", .value_json = "\"Ada Lovelace\"" },
            .{ .op = .add_to_set, .path = "aliases", .value_json = "\"Ada\"" },
        },
    };

    const inserted = try resolveDocumentTransform(alloc, null, transform);
    defer alloc.free(inserted.?);
    var inserted_parsed = try std.json.parseFromSlice(std.json.Value, alloc, inserted.?, .{});
    defer inserted_parsed.deinit();
    try std.testing.expectEqualStrings("Ada Lovelace", inserted_parsed.value.object.get("canonical_name").?.string);

    const existing = try resolveDocumentTransform(alloc, "{\"canonical_name\":\"Countess of Lovelace\",\"aliases\":[\"A. A. L.\"]}", transform);
    defer alloc.free(existing.?);
    var existing_parsed = try std.json.parseFromSlice(std.json.Value, alloc, existing.?, .{});
    defer existing_parsed.deinit();
    try std.testing.expectEqualStrings("Countess of Lovelace", existing_parsed.value.object.get("canonical_name").?.string);
    try std.testing.expectEqual(@as(usize, 2), existing_parsed.value.object.get("aliases").?.array.items.len);
}

test "resolve document transform skips missing document without upsert" {
    const alloc = std.testing.allocator;
    const transform: types.DocumentTransform = .{
        .key = "doc:missing",
        .operations = &.{.{ .op = .set, .path = "status", .value_json = "\"new\"" }},
    };
    const resolved = try resolveDocumentTransform(alloc, null, transform);
    try std.testing.expect(resolved == null);
}

test "unsupported transforms fail atomically instead of reporting success" {
    const alloc = std.testing.allocator;
    const unsupported = [_]types.TransformOpType{
        .push, .pull, .pop, .mul, .min, .current_date, .rename,
    };
    for (unsupported) |op| {
        const operations = [_]types.TransformOp{
            .{ .op = .set, .path = "changed", .value_json = "true" },
            .{ .op = op, .path = "n", .value_json = "3" },
        };
        try std.testing.expectError(
            error.UnsupportedTransformOperation,
            resolveDocumentTransform(alloc, "{\"n\":9}", .{
                .key = "doc",
                .operations = &operations,
            }),
        );
    }
}

test "unsupported transform on a missing document is rejected before no-op resolution" {
    const operations = [_]types.TransformOp{
        .{ .op = .push, .path = "tags", .value_json = "\"new\"" },
    };
    try std.testing.expectError(
        error.UnsupportedTransformOperation,
        resolveDocumentTransform(std.testing.allocator, null, .{
            .key = "doc:missing",
            .operations = &operations,
        }),
    );
}
