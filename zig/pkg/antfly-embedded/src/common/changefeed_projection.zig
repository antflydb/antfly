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

//! Exact filtered document effects over already authorized canonical images.
//! The provider must evaluate live row/field policy for BOTH historical images,
//! remove private storage fields, and reset on policy revision changes. This
//! selector is not an authorization implementation or a current-row lookup.
const std = @import("std");
const numbers = @import("json_number.zig");
const A = std.mem.Allocator;
const Value = std.json.Value;
pub const Projection = enum { keys_and_versions, rows };
pub const Operation = enum { insert, enter, update, leave, delete };
pub const Image = struct { row: ?Value = null, readable: bool = true };
pub const Effect = struct { operation: Operation, before: ?Value = null, after: ?Value = null };
fn text(value: []const u8, max: usize) bool {
    return value.len != 0 and value.len <= max and std.unicode.utf8ValidateSlice(value);
}
fn scalar(value: Value) bool {
    return switch (value) {
        .null, .bool, .integer => true,
        .float => |v| std.math.isFinite(v),
        .string => |v| v.len <= 4096 and std.unicode.utf8ValidateSlice(v),
        .number_string => |v| v.len <= 4096 and numbers.Number.parse(v) != null,
        else => false,
    };
}
/// Selectors use literal top-level field names, sorted and unique. The caller
/// must have separately authorized both projected and predicate fields.
pub fn validate(selector: anytype) !void {
    if (selector.fields.len > 64 or selector.predicates.len > 16) return error.InvalidChangefeed;
    for (selector.fields, 0..) |field, index| if (!text(field, 255) or (index != 0 and std.mem.order(u8, selector.fields[index - 1], field) != .lt)) return error.InvalidChangefeed;
    for (selector.predicates, 0..) |predicate, index| if (!text(predicate.field, 255) or !scalar(predicate.value) or (index != 0 and std.mem.order(u8, selector.predicates[index - 1].field, predicate.field) != .lt)) return error.InvalidChangefeed;
}
fn equalScalar(left: Value, right: Value) bool {
    var left_buffer: [64]u8 = undefined;
    var right_buffer: [64]u8 = undefined;
    if (numbers.fromValue(left, &left_buffer)) |a| {
        const b = numbers.fromValue(right, &right_buffer) orelse return false;
        return numbers.Number.order(a, b) == .eq;
    }
    return switch (left) {
        .null => right == .null,
        .bool => |v| right == .bool and right.bool == v,
        .string => |v| right == .string and std.mem.eql(u8, v, right.string),
        else => false,
    };
}
fn visible(image: Image) !?Value {
    // Hidden existence must not affect operation classification or validation.
    if (!image.readable) return null;
    const row = image.row orelse return null;
    if (row != .object) return error.ChangefeedCapabilityUnsupported;
    return row;
}
fn matches(selector: anytype, row: ?Value) bool {
    const object = (row orelse return false).object;
    for (selector.predicates) |predicate| {
        // Missing is distinct from explicit JSON null.
        const actual = object.get(predicate.field) orelse return false;
        if (!equalScalar(actual, predicate.value)) return false;
    }
    return true;
}
/// Iterative arena-owned clone. Nested JSON cannot consume the native call stack
/// or lose decimal precision through a stringify/float round trip. Caller frees
/// the entire attempt arena; individual result values must not be deinitialized.
fn clone(a: A, input: Value) !Value {
    const Task = struct { input: Value, output: *Value };
    var tasks: std.ArrayList(Task) = .empty;
    defer tasks.deinit(a);
    var output: Value = undefined;
    try tasks.append(a, .{ .input = input, .output = &output });
    while (tasks.pop()) |task| switch (task.input) {
        .string => |v| task.output.* = .{ .string = try a.dupe(u8, v) },
        .number_string => |v| task.output.* = .{ .number_string = try a.dupe(u8, v) },
        .array => |array| {
            var copied = std.json.Array.init(a);
            try copied.ensureTotalCapacity(array.items.len);
            for (array.items) |_| copied.appendAssumeCapacity(.null);
            task.output.* = .{ .array = copied };
            for (array.items, copied.items) |value, *destination| try tasks.append(a, .{ .input = value, .output = destination });
        },
        .object => |object| {
            var copied: std.json.ObjectMap = .empty;
            try copied.ensureTotalCapacity(a, @intCast(object.count()));
            for (object.keys()) |name| try copied.put(a, try a.dupe(u8, name), .null);
            task.output.* = .{ .object = copied };
            for (object.values(), copied.values()) |value, *destination| try tasks.append(a, .{ .input = value, .output = destination });
        },
        else => task.output.* = task.input,
    };
    return output;
}
fn select(a: A, selector: anytype, row: Value) !Value {
    if (selector.fields.len == 0) return clone(a, row);
    var object: std.json.ObjectMap = .empty;
    for (selector.fields) |field| {
        if (row.object.get(field)) |value| try object.put(a, try a.dupe(u8, field), try clone(a, value));
    }
    return .{ .object = object };
}
/// A selected update remains an update even if only unselected fields changed;
/// this default contract retains committed version notifications. Filtering
/// uses full authorized BEFORE/AFTER rows before applying field projection.
pub fn project(a: A, selector: anytype, projection: Projection, before_image: Image, after_image: Image) !?Effect {
    try validate(selector);
    const before = try visible(before_image);
    const after = try visible(after_image);
    const was = matches(selector, before);
    const now = matches(selector, after);
    if (!was and !now) return null;
    const operation: Operation = if (was and now) .update else if (now) (if (before == null) .insert else .enter) else (if (after == null) .delete else .leave);
    return .{
        .operation = operation,
        .before = if (projection == .rows and was) try select(a, selector, before.?) else null,
        .after = if (projection == .rows and now) try select(a, selector, after.?) else null,
    };
}

const TestPredicate = struct { field: []const u8, value: Value };
const TestSelector = struct { fields: []const []const u8 = &.{}, predicates: []const TestPredicate = &.{} };
fn json(a: A, bytes: []const u8) !Value {
    return std.json.parseFromSliceLeaky(Value, a, bytes, .{ .parse_numbers = false, .allocate = .alloc_always });
}

test "feed projection preserves insert enter update leave and delete using both committed images" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const yes = try json(a, "{\"match\":true,\"shown\":1,\"not_selected\":\"private\"}");
    const newer = try json(a, "{\"match\":true,\"shown\":2}");
    const no = try json(a, "{\"match\":false,\"shown\":3}");
    const selector: TestSelector = .{ .fields = &.{"shown"}, .predicates = &.{.{ .field = "match", .value = .{ .bool = true } }} };
    const inserted = (try project(a, selector, .rows, .{}, .{ .row = yes })).?;
    try std.testing.expectEqual(Operation.insert, inserted.operation);
    try std.testing.expect(inserted.before == null);
    try std.testing.expectEqual(@as(usize, 1), inserted.after.?.object.count());
    try std.testing.expect(!inserted.after.?.object.contains("match"));
    try std.testing.expectEqual(Operation.enter, (try project(a, selector, .rows, .{ .row = no }, .{ .row = yes })).?.operation);
    const updated = (try project(a, selector, .rows, .{ .row = yes }, .{ .row = newer })).?;
    try std.testing.expectEqual(Operation.update, updated.operation);
    try std.testing.expectEqualStrings("1", updated.before.?.object.get("shown").?.number_string);
    try std.testing.expectEqualStrings("2", updated.after.?.object.get("shown").?.number_string);
    const left = (try project(a, selector, .rows, .{ .row = yes }, .{ .row = no })).?;
    try std.testing.expectEqual(Operation.leave, left.operation);
    try std.testing.expect(left.after == null and left.before != null);
    try std.testing.expectEqual(Operation.delete, (try project(a, selector, .rows, .{ .row = yes }, .{})).?.operation);
    try std.testing.expectEqual(@as(?Effect, null), try project(a, selector, .rows, .{ .row = no }, .{}));
    const keys = (try project(a, selector, .keys_and_versions, .{ .row = yes }, .{ .row = newer })).?;
    try std.testing.expect(keys.before == null and keys.after == null);
}

test "feed projection exact decimal predicates preserve precision and null versus missing" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var selector: TestSelector = .{ .predicates = &.{.{ .field = "n", .value = .{ .number_string = "9007199254740993" } }} };
    const row = try json(a, "{\"n\":9007199254740993.00e0}");
    try std.testing.expect((try project(a, selector, .rows, .{}, .{ .row = row })) != null);
    const other = try json(a, "{\"n\":9007199254740992}");
    try std.testing.expect((try project(a, selector, .rows, .{}, .{ .row = other })) == null);
    selector.predicates = &.{.{ .field = "n", .value = .{ .number_string = "1e999999999999999999999999999999999" } }};
    const huge = try json(a, "{\"n\":10e999999999999999999999999999999998}");
    try std.testing.expect((try project(a, selector, .rows, .{}, .{ .row = huge })) != null);
    selector.predicates = &.{.{ .field = "n", .value = .null }};
    try std.testing.expect((try project(a, selector, .rows, .{}, .{ .row = try json(a, "{}") })) == null);
    try std.testing.expect((try project(a, selector, .rows, .{}, .{ .row = try json(a, "{\"n\":null}") })) != null);
}

test "feed projection cannot expose inaccessible image existence or excluded values" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const row = try json(a, "{\"allowed\":1}");
    const hidden = try json(a, "[\"inaccessible and unsupported\"]");
    const selector: TestSelector = .{};
    const entered = (try project(a, selector, .rows, .{ .row = hidden, .readable = false }, .{ .row = row })).?;
    try std.testing.expectEqual(Operation.insert, entered.operation);
    try std.testing.expect(entered.before == null);
    const left = (try project(a, selector, .rows, .{ .row = row }, .{ .row = hidden, .readable = false })).?;
    try std.testing.expectEqual(Operation.delete, left.operation);
    try std.testing.expect(left.after == null);
    try std.testing.expect((try project(a, selector, .rows, .{ .row = hidden, .readable = false }, .{ .row = hidden, .readable = false })) == null);
}

test "feed projection clones deeply nested owned images without recursion or float conversion" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var nested: Value = .{ .number_string = "18446744073709551617" };
    for (0..3000) |_| {
        var array = std.json.Array.init(a);
        try array.append(nested);
        nested = .{ .array = array };
    }
    var row: std.json.ObjectMap = .empty;
    try row.put(a, "nested", nested);
    const result = (try project(a, TestSelector{}, .rows, .{}, .{ .row = .{ .object = row } })).?;
    var value = result.after.?.object.get("nested").?;
    for (0..3000) |_| value = value.array.items[0];
    try std.testing.expectEqualStrings("18446744073709551617", value.number_string);
}
