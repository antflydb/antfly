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
    var prepared = try prepareDocumentTransform(alloc, transform);
    defer prepared.deinit(alloc);
    if (existing_json == null and !transform.upsert) return null;
    const is_insert = existing_json == null;

    var root = if (existing_json) |body| blk: {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
        defer parsed.deinit();
        if (parsed.value != .object) return error.InvalidArgument;
        break :blk try cloneJsonValue(alloc, parsed.value);
    } else std.json.Value{ .object = std.json.ObjectMap.empty };
    defer freeJsonValue(alloc, &root);

    for (prepared.operations) |*op| try applyPreparedTransformOp(alloc, &root, op, is_insert);

    return try std.json.Stringify.valueAlloc(alloc, root, .{});
}

/// Validates the complete transform at admission and storage boundaries.
///
/// Keep this independent of document state so an unsupported request cannot
/// become an acknowledged no-op merely because its target is absent.
pub fn validateDocumentTransform(alloc: Allocator, transform: types.DocumentTransform) !void {
    for (transform.operations) |op| {
        var prepared = try prepareTransformOp(alloc, op, false);
        prepared.deinit(alloc);
    }
}

pub fn validateTransformOpType(op: types.TransformOpType) !void {
    switch (op) {
        .set, .set_on_insert, .unset, .inc, .push, .add_to_set, .min, .max => {},
        else => return error.UnsupportedTransformOperation,
    }
}

pub fn transformOpText(op: types.TransformOpType) []const u8 {
    return types.transformOpText(op);
}

const PreparedValue = union(enum) {
    none,
    json: std.json.Value,
    number: NumericValue,
};

const NumericValue = union(enum) {
    integer: i64,
    float: f64,

    fn fromJson(value: std.json.Value) !NumericValue {
        return switch (value) {
            .integer => |number| .{ .integer = number },
            .float => |number| if (std.math.isFinite(number))
                .{ .float = number }
            else
                error.InvalidArgument,
            else => error.InvalidArgument,
        };
    }

    fn asFloat(self: NumericValue) f64 {
        return switch (self) {
            .integer => |number| @floatFromInt(number),
            .float => |number| number,
        };
    }

    fn toJson(self: NumericValue) std.json.Value {
        return switch (self) {
            .integer => |number| .{ .integer = number },
            .float => |number| .{ .float = number },
        };
    }
};

const PreparedTransformOp = struct {
    op: types.TransformOpType,
    path: NormalizedJsonPath,
    value: PreparedValue = .none,

    fn deinit(self: *PreparedTransformOp, alloc: Allocator) void {
        if (self.value == .json) freeJsonValue(alloc, &self.value.json);
        self.* = undefined;
    }

    fn takeJson(self: *PreparedTransformOp) !std.json.Value {
        const value = switch (self.value) {
            .json => |value| value,
            else => return error.InvalidArgument,
        };
        self.value = .none;
        return value;
    }

    fn number(self: PreparedTransformOp) !NumericValue {
        return switch (self.value) {
            .number => |value| value,
            else => error.InvalidArgument,
        };
    }
};

const PreparedDocumentTransform = struct {
    operations: []PreparedTransformOp,

    fn deinit(self: *PreparedDocumentTransform, alloc: Allocator) void {
        for (self.operations) |*op| op.deinit(alloc);
        if (self.operations.len > 0) alloc.free(self.operations);
        self.* = undefined;
    }
};

fn prepareDocumentTransform(
    alloc: Allocator,
    transform: types.DocumentTransform,
) !PreparedDocumentTransform {
    const operations = try alloc.alloc(PreparedTransformOp, transform.operations.len);
    var initialized: usize = 0;
    errdefer {
        for (operations[0..initialized]) |*op| op.deinit(alloc);
        if (operations.len > 0) alloc.free(operations);
    }

    for (transform.operations, operations) |op, *prepared| {
        prepared.* = try prepareTransformOp(alloc, op, true);
        initialized += 1;
    }
    return .{ .operations = operations };
}

fn prepareTransformOp(
    alloc: Allocator,
    op: types.TransformOp,
    retain_json: bool,
) !PreparedTransformOp {
    try validateTransformOpType(op.op);
    const path = try normalizeJsonPath(op.path);
    if (path.len == 0) return error.InvalidArgument;

    var prepared = PreparedTransformOp{ .op = op.op, .path = path };
    errdefer prepared.deinit(alloc);
    switch (op.op) {
        .unset => {
            if (op.value_json != null) return error.InvalidArgument;
        },
        .set, .set_on_insert, .push, .add_to_set => {
            const value_json = op.value_json orelse return error.InvalidArgument;
            var parsed = std.json.parseFromSlice(
                std.json.Value,
                alloc,
                value_json,
                .{},
            ) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => return error.InvalidArgument,
            };
            defer parsed.deinit();
            if (retain_json) {
                prepared.value = .{ .json = try cloneJsonValue(alloc, parsed.value) };
            }
        },
        .inc, .min, .max => {
            const value_json = op.value_json orelse return error.InvalidArgument;
            var parsed = std.json.parseFromSlice(
                std.json.Value,
                alloc,
                value_json,
                .{},
            ) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => return error.InvalidArgument,
            };
            defer parsed.deinit();
            prepared.value = .{ .number = try NumericValue.fromJson(parsed.value) };
        },
        else => unreachable,
    }
    return prepared;
}

fn applyPreparedTransformOp(
    alloc: Allocator,
    root: *std.json.Value,
    op: *PreparedTransformOp,
    is_insert: bool,
) !void {
    if (root.* != .object) return error.InvalidArgument;
    const path = op.path.slice();
    switch (op.op) {
        .set => {
            var value = try op.takeJson();
            errdefer freeJsonValue(alloc, &value);
            try setNestedValue(alloc, &root.object, path, value);
        },
        .set_on_insert => if (is_insert) {
            var value = try op.takeJson();
            errdefer freeJsonValue(alloc, &value);
            try setNestedValue(alloc, &root.object, path, value);
        },
        .unset => removeNestedValue(alloc, &root.object, path),
        .inc => try applyNumericOp(alloc, &root.object, path, try op.number(), .add),
        .push => {
            var value = try op.takeJson();
            try pushPreparedValue(alloc, &root.object, path, &value);
        },
        .add_to_set => {
            var value = try op.takeJson();
            try addPreparedValueToSet(alloc, &root.object, path, &value);
        },
        .min => try applyNumericOp(alloc, &root.object, path, try op.number(), .min),
        .max => try applyNumericOp(alloc, &root.object, path, try op.number(), .max),
        else => return error.UnsupportedTransformOperation,
    }
}

fn pushPreparedValue(
    alloc: Allocator,
    obj: *std.json.ObjectMap,
    parts: []const []const u8,
    value: *std.json.Value,
) !void {
    errdefer freeJsonValue(alloc, value);
    if (parts.len == 0) return error.InvalidArgument;
    if (getNestedValue(obj, parts)) |existing| {
        if (existing.* != .array) return error.InvalidArgument;
        try existing.array.append(value.*);
        value.* = .null;
        return;
    }

    var arr = std.json.Array.init(alloc);
    errdefer {
        for (arr.items) |*item| freeJsonValue(alloc, item);
        arr.deinit();
    }
    try arr.append(value.*);
    value.* = .null;
    try setNestedValue(alloc, obj, parts, .{ .array = arr });
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

pub const GraphProjectionPath = struct {
    index_name: []const u8,
    edge_type: []const u8,
};

/// Recognizes the logical graph-array paths exposed by document transforms.
///
/// `_edges` is projected out of the primary document and stored as graph edge
/// artifacts. Callers must therefore apply append-like operations as graph
/// deltas instead of resolving them against the stripped primary JSON. Any
/// other path rooted at `_edges` is rejected: resolving it as ordinary JSON
/// would silently reinterpret a partial projection as the complete edge set.
pub fn graphProjectionPath(path: []const u8) !?GraphProjectionPath {
    const normalized = try normalizeJsonPath(path);
    const parts = normalized.slice();
    if (parts.len == 0 or !std.mem.eql(u8, parts[0], "_edges")) return null;
    if (parts.len != 3) return error.InvalidArgument;
    return .{
        .index_name = parts[1],
        .edge_type = parts[2],
    };
}

const NumericTransform = enum { add, min, max };

fn applyNumericOp(
    alloc: Allocator,
    obj: *std.json.ObjectMap,
    parts: []const []const u8,
    operand: NumericValue,
    operation: NumericTransform,
) !void {
    if (parts.len == 0) return error.InvalidArgument;
    if (getNestedValue(obj, parts)) |current| {
        const current_num = try NumericValue.fromJson(current.*);
        const next = switch (operation) {
            .add => try addNumericValues(current_num, operand),
            .min => if (compareNumericValues(operand, current_num) == .lt) operand else return,
            .max => if (compareNumericValues(operand, current_num) == .gt) operand else return,
        };
        try setNestedValue(alloc, obj, parts, next.toJson());
        return;
    }
    try setNestedValue(alloc, obj, parts, operand.toJson());
}

fn addNumericValues(left: NumericValue, right: NumericValue) !NumericValue {
    if (left == .integer and right == .integer) {
        return .{ .integer = std.math.add(i64, left.integer, right.integer) catch return error.InvalidArgument };
    }
    const sum = left.asFloat() + right.asFloat();
    if (!std.math.isFinite(sum)) return error.InvalidArgument;
    return .{ .float = sum };
}

fn compareNumericValues(left: NumericValue, right: NumericValue) std.math.Order {
    return switch (left) {
        .integer => |left_integer| switch (right) {
            .integer => |right_integer| std.math.order(left_integer, right_integer),
            .float => |right_float| compareIntegerToFloat(left_integer, right_float),
        },
        .float => |left_float| switch (right) {
            .integer => |right_integer| reverseOrder(compareIntegerToFloat(right_integer, left_float)),
            .float => |right_float| std.math.order(left_float, right_float),
        },
    };
}

fn compareIntegerToFloat(left: i64, right: f64) std.math.Order {
    const lower_bound: f64 = @floatFromInt(std.math.minInt(i64));
    const upper_bound_exclusive = -lower_bound;
    if (right < lower_bound) return .gt;
    if (right >= upper_bound_exclusive) return .lt;

    const truncated: i64 = @intFromFloat(right);
    const integer_order = std.math.order(left, truncated);
    if (integer_order != .eq) return integer_order;
    return std.math.order(@as(f64, @floatFromInt(truncated)), right);
}

fn reverseOrder(order: std.math.Order) std.math.Order {
    return switch (order) {
        .lt => .gt,
        .eq => .eq,
        .gt => .lt,
    };
}

fn addPreparedValueToSet(
    alloc: Allocator,
    obj: *std.json.ObjectMap,
    parts: []const []const u8,
    value: *std.json.Value,
) !void {
    errdefer freeJsonValue(alloc, value);

    if (parts.len == 0) return error.InvalidArgument;
    if (getNestedValue(obj, parts)) |existing| {
        switch (existing.*) {
            .array => |*arr| {
                for (arr.items) |item| {
                    if (jsonValuesEqual(item, value.*)) {
                        freeJsonValue(alloc, value);
                        return;
                    }
                }
                try arr.append(value.*);
                value.* = .null;
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
    try arr.append(value.*);
    value.* = .null;
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
            if (current.getPtr(part)) |existing| {
                if (existing.* != .object) {
                    freeJsonValue(alloc, existing);
                    existing.* = .{ .object = std.json.ObjectMap.empty };
                }
                current = &existing.object;
                continue;
            }

            const owned_part = try alloc.dupe(u8, part);
            errdefer alloc.free(owned_part);
            try current.putNoClobber(
                alloc,
                owned_part,
                .{ .object = std.json.ObjectMap.empty },
            );
            current = &current.getPtr(part).?.object;
        }
    }
    const leaf = parts[parts.len - 1];
    if (current.getPtr(leaf)) |existing| {
        freeJsonValue(alloc, existing);
        existing.* = value;
        return;
    }
    const owned_leaf = try alloc.dupe(u8, leaf);
    errdefer alloc.free(owned_leaf);
    try current.putNoClobber(alloc, owned_leaf, value);
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

fn jsonNumberFromValue(value: std.json.Value) !f64 {
    return (try NumericValue.fromJson(value)).asFloat();
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

test "resolve document transform supports set setOnInsert min max inc push and addToSet" {
    const alloc = std.testing.allocator;

    const transform: types.DocumentTransform = .{
        .key = "doc:1",
        .upsert = false,
        .operations = &.{
            .{ .op = .set_on_insert, .path = "owner", .value_json = "\"system\"" },
            .{ .op = .min, .path = "priority", .value_json = "3" },
            .{ .op = .max, .path = "version", .value_json = "10" },
            .{ .op = .set, .path = "status", .value_json = "\"updated\"" },
            .{ .op = .inc, .path = "views", .value_json = "2" },
            .{ .op = .push, .path = "events", .value_json = "{\"type\":\"published\"}" },
            .{ .op = .add_to_set, .path = "tags", .value_json = "\"zig\"" },
        },
    };

    const resolved = try resolveDocumentTransform(
        alloc,
        "{\"priority\":5,\"version\":5,\"views\":1,\"tags\":[\"db\"]}",
        transform,
    );
    defer alloc.free(resolved.?);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, resolved.?, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(f64, 3), try jsonNumberFromValue(parsed.value.object.get("priority").?));
    try std.testing.expectEqual(@as(f64, 10), try jsonNumberFromValue(parsed.value.object.get("version").?));
    try std.testing.expectEqual(@as(f64, 3), try jsonNumberFromValue(parsed.value.object.get("views").?));
    try std.testing.expectEqualStrings("updated", parsed.value.object.get("status").?.string);
    try std.testing.expect(parsed.value.object.get("owner") == null);
    try std.testing.expectEqual(@as(usize, 2), parsed.value.object.get("tags").?.array.items.len);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.object.get("events").?.array.items.len);
    try std.testing.expectEqualStrings("published", parsed.value.object.get("events").?.array.items[0].object.get("type").?.string);
}

test "min only lowers numeric values and follows missing document semantics" {
    const alloc = std.testing.allocator;
    const lower = [_]types.TransformOp{
        .{ .op = .min, .path = "score", .value_json = "5" },
        .{ .op = .min, .path = "unchanged", .value_json = "12" },
        .{ .op = .min, .path = "missing", .value_json = "7" },
    };
    const resolved = (try resolveDocumentTransform(alloc, "{\"score\":10,\"unchanged\":8}", .{
        .key = "doc:existing",
        .operations = &lower,
    })).?;
    defer alloc.free(resolved);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, resolved, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(f64, 5), try jsonNumberFromValue(parsed.value.object.get("score").?));
    try std.testing.expectEqual(@as(f64, 8), try jsonNumberFromValue(parsed.value.object.get("unchanged").?));
    try std.testing.expectEqual(@as(f64, 7), try jsonNumberFromValue(parsed.value.object.get("missing").?));

    try std.testing.expectError(
        error.InvalidArgument,
        resolveDocumentTransform(alloc, "{\"score\":\"high\"}", .{
            .key = "doc:non-numeric",
            .operations = lower[0..1],
        }),
    );

    try std.testing.expect((try resolveDocumentTransform(alloc, null, .{
        .key = "doc:missing",
        .operations = lower[0..1],
    })) == null);

    const upserted = (try resolveDocumentTransform(alloc, null, .{
        .key = "doc:new",
        .operations = lower[0..1],
        .upsert = true,
    })).?;
    defer alloc.free(upserted);
    var upserted_parsed = try std.json.parseFromSlice(std.json.Value, alloc, upserted, .{});
    defer upserted_parsed.deinit();
    try std.testing.expectEqual(@as(f64, 5), try jsonNumberFromValue(upserted_parsed.value.object.get("score").?));
}

test "numeric transforms preserve integers exactly beyond f64 precision" {
    const alloc = std.testing.allocator;
    const operations = [_]types.TransformOp{
        .{ .op = .min, .path = "lowered", .value_json = "9007199254740992" },
        .{ .op = .min, .path = "unchanged", .value_json = "9007199254740993" },
        .{ .op = .max, .path = "raised", .value_json = "9007199254740993" },
        .{ .op = .inc, .path = "incremented", .value_json = "1" },
    };
    const resolved = (try resolveDocumentTransform(
        alloc,
        "{\"lowered\":9007199254740993,\"unchanged\":9007199254740992,\"raised\":9007199254740992,\"incremented\":9007199254740992}",
        .{ .key = "doc:large-integers", .operations = &operations },
    )).?;
    defer alloc.free(resolved);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, resolved, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, 9007199254740992), parsed.value.object.get("lowered").?.integer);
    try std.testing.expectEqual(@as(i64, 9007199254740992), parsed.value.object.get("unchanged").?.integer);
    try std.testing.expectEqual(@as(i64, 9007199254740993), parsed.value.object.get("raised").?.integer);
    try std.testing.expectEqual(@as(i64, 9007199254740993), parsed.value.object.get("incremented").?.integer);

    try std.testing.expectEqual(
        std.math.Order.gt,
        compareNumericValues(.{ .integer = 9007199254740993 }, .{ .float = 9007199254740992.0 }),
    );
}

test "push appends to existing arrays and rejects non-array targets" {
    const alloc = std.testing.allocator;
    const operations = [_]types.TransformOp{
        .{ .op = .push, .path = "edges", .value_json = "\"doc:b\"" },
    };
    const transform: types.DocumentTransform = .{ .key = "doc:a", .operations = &operations };

    const resolved = (try resolveDocumentTransform(alloc, "{\"edges\":[\"doc:z\"]}", transform)).?;
    defer alloc.free(resolved);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, resolved, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed.value.object.get("edges").?.array.items.len);
    try std.testing.expectEqualStrings("doc:b", parsed.value.object.get("edges").?.array.items[1].string);

    try std.testing.expectError(
        error.InvalidArgument,
        resolveDocumentTransform(alloc, "{\"edges\":\"doc:z\"}", transform),
    );
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
        .pull, .pop, .mul, .current_date, .rename,
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

test "supported transform on a missing document remains a no-op without upsert" {
    const operations = [_]types.TransformOp{
        .{ .op = .push, .path = "tags", .value_json = "\"new\"" },
    };
    try std.testing.expect((try resolveDocumentTransform(std.testing.allocator, null, .{
        .key = "doc:missing",
        .operations = &operations,
    })) == null);
}

test "invalid transform shape is rejected before missing-document no-op resolution" {
    const missing_value = [_]types.TransformOp{
        .{ .op = .set, .path = "status" },
    };
    try std.testing.expectError(
        error.InvalidArgument,
        resolveDocumentTransform(std.testing.allocator, null, .{
            .key = "doc:missing",
            .operations = &missing_value,
        }),
    );

    const unset_value = [_]types.TransformOp{
        .{ .op = .unset, .path = "status", .value_json = "true" },
    };
    try std.testing.expectError(
        error.InvalidArgument,
        resolveDocumentTransform(std.testing.allocator, null, .{
            .key = "doc:missing",
            .operations = &unset_value,
        }),
    );

    const invalid_path = [_]types.TransformOp{
        .{ .op = .set, .path = "$", .value_json = "true" },
    };
    try std.testing.expectError(
        error.InvalidArgument,
        resolveDocumentTransform(std.testing.allocator, null, .{
            .key = "doc:missing",
            .operations = &invalid_path,
        }),
    );
}

test "invalid transform operands are rejected before missing-document no-op resolution" {
    const malformed_json = [_]types.TransformOp{
        .{ .op = .set, .path = "status", .value_json = "{\"unterminated\":" },
    };
    try std.testing.expectError(
        error.InvalidArgument,
        resolveDocumentTransform(std.testing.allocator, null, .{
            .key = "doc:missing",
            .operations = &malformed_json,
        }),
    );

    const non_numeric_increment = [_]types.TransformOp{
        .{ .op = .inc, .path = "count", .value_json = "\"one\"" },
    };
    try std.testing.expectError(
        error.InvalidArgument,
        resolveDocumentTransform(std.testing.allocator, null, .{
            .key = "doc:missing",
            .operations = &non_numeric_increment,
        }),
    );

    const non_numeric_min = [_]types.TransformOp{
        .{ .op = .min, .path = "count", .value_json = "\"one\"" },
    };
    try std.testing.expectError(
        error.InvalidArgument,
        resolveDocumentTransform(std.testing.allocator, null, .{
            .key = "doc:missing",
            .operations = &non_numeric_min,
        }),
    );

    const overflowing_increment = [_]types.TransformOp{
        .{ .op = .inc, .path = "count", .value_json = "1e308" },
    };
    try std.testing.expectError(
        error.InvalidArgument,
        resolveDocumentTransform(std.testing.allocator, "{\"count\":1e308}", .{
            .key = "doc:overflow",
            .operations = &overflowing_increment,
        }),
    );

    const overflowing_integer_increment = [_]types.TransformOp{
        .{ .op = .inc, .path = "count", .value_json = "1" },
    };
    try std.testing.expectError(
        error.InvalidArgument,
        resolveDocumentTransform(std.testing.allocator, "{\"count\":9223372036854775807}", .{
            .key = "doc:integer-overflow",
            .operations = &overflowing_integer_increment,
        }),
    );
}
