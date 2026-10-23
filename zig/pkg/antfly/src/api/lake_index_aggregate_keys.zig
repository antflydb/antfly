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

//! Conservative fixed-size fences over the native scalar tuple codec. The
//! complete key remains authoritative; truncated fences only widen scans.
const std = @import("std");
const local = @import("antfly_local_sources");
const Datum = local.sql_scalar.Datum;
const A = std.mem.Allocator;
pub const Prefix = struct {
    bytes: [64]u8 = @splat(0),
    len: u8 = 0,
    pub fn slice(self: *const Prefix) []const u8 {
        return self.bytes[0..self.len];
    }
    fn from(bytes: []const u8) Prefix {
        var result: Prefix = .{ .len = @intCast(@min(64, bytes.len)) };
        @memcpy(result.bytes[0..result.len], bytes[0..result.len]);
        return result;
    }
};
pub const Bounds = struct {
    lower: Prefix,
    upper: ?Prefix,
    pub fn validate(self: Bounds) !void {
        if (self.lower.len == 0 or self.lower.len > 64) return error.InvalidNativeAggregateArtifact;
        if (self.upper) |upper| if (upper.len == 0 or upper.len > 64 or std.mem.order(u8, self.lower.slice(), upper.slice()) != .lt) return error.InvalidNativeAggregateArtifact;
    }
};
pub const Range = struct {
    lower: []const u8 = "",
    upper: ?[]const u8 = null,
    pub fn contains(self: Range, key: []const u8) bool {
        return std.mem.order(u8, key, self.lower) != .lt and (self.upper == null or std.mem.order(u8, key, self.upper.?) == .lt);
    }
    pub fn overlaps(self: Range, bounds: ?Bounds) bool {
        const b = bounds orelse return true;
        return (self.upper == null or std.mem.order(u8, b.lower.slice(), self.upper.?) == .lt) and (b.upper == null or std.mem.order(u8, self.lower, b.upper.?.slice()) == .lt);
    }
};
pub fn successor(a: A, bytes: []const u8) !?[]const u8 {
    var n = bytes.len;
    while (n != 0) {
        n -= 1;
        if (bytes[n] == 255) continue;
        const result = try a.dupe(u8, bytes[0 .. n + 1]);
        result[n] += 1;
        return result;
    }
    return null;
}
pub fn encode(a: A, values: []const Datum) ![]const u8 {
    var bytes: std.ArrayList(u8) = .empty;
    errdefer bytes.deinit(a);
    for (values) |value| try local.storage_db_relational_index_keys.appendSqlScalar(a, &bytes, value, false);
    return bytes.toOwnedSlice(a);
}
pub fn compatible(recipe: local.sql_aggregate_materialization.Recipe, values: []const Datum) bool {
    if (values.len != recipe.keys.len) return false;
    for (recipe.keys, values) |key, value| if ((value.sql_null and !key.nullable) or !typed(key, value)) return false;
    return true;
}
pub fn rowBoundsTyped(a: A, rows: []const local.sql_operators.Row, recipe: local.sql_aggregate_materialization.Recipe) !?Bounds {
    for (rows) |row| {
        if (row.keys.len != recipe.keys.len) return error.InvalidNativeAggregateArtifact;
        for (recipe.keys, row.keys) |key, value| if ((value.sql_null and !key.nullable) or !typed(key, value)) return null;
    }
    return rowBounds(a, rows);
}
pub fn rowBounds(a: A, rows: []const local.sql_operators.Row) !?Bounds {
    if (rows.len == 0 or rows[0].keys.len == 0) return null;
    var budget: local.sql_memory_budget = .{ .backing = local.sql_memory_budget.reclaimingAllocator(a), .limit = 256 * 1024 };
    var scratch = std.heap.ArenaAllocator.init(budget.allocator());
    defer scratch.deinit();
    var result: ?Bounds = null;
    for (rows) |row| {
        _ = scratch.reset(.free_all);
        const bytes = encode(scratch.allocator(), row.keys) catch |err| {
            if (err == error.OutOfMemory and budget.isLocallyExhausted()) return null;
            if (err == error.InvalidRelationalIndexBound or err == error.InvalidColumnValue or err == error.RelationalIndexKeyTooLarge) return null;
            return err;
        };
        const lower = Prefix.from(bytes);
        var upper = lower;
        var n: usize = upper.len;
        while (n != 0 and upper.bytes[n - 1] == 255) n -= 1;
        const ceiling: ?Prefix = if (n == 0) null else blk: {
            upper.len = @intCast(n);
            upper.bytes[n - 1] += 1;
            @memset(upper.bytes[n..], 0);
            break :blk upper;
        };
        result = envelope(result, .{ .lower = lower, .upper = ceiling });
    }
    return result;
}
pub fn envelope(left: ?Bounds, right: Bounds) Bounds {
    const l = left orelse return right;
    return .{ .lower = if (std.mem.order(u8, l.lower.slice(), right.lower.slice()) == .lt) l.lower else right.lower, .upper = if (l.upper == null or right.upper == null) null else if (std.mem.order(u8, l.upper.?.slice(), right.upper.?.slice()) == .gt) l.upper else right.upper };
}
fn typed(key: local.sql_aggregate_materialization.Key, value: Datum) bool {
    if (value.sql_null) return true;
    return switch (key.type) {
        .integer => value.numeric == null and value.value == .integer,
        .string, .datetime => value.numeric == null and value.value == .string,
        .boolean => value.numeric == null and value.value == .bool,
        .number => if (key.element_type == .numeric) value.numeric != null else value.numeric == null and value.value == .float,
        else => false,
    };
}
/// Equality prefixes followed by one ordered range. All other conditions stay
/// with SQL's residual predicate, including mixed numeric comparison domains.
pub fn range(a: A, recipe: local.sql_aggregate_materialization.Recipe, conditions: []const local.sql_catalog.Condition) !Range {
    var budget: local.sql_memory_budget = .{ .backing = local.sql_memory_budget.reclaimingAllocator(a), .limit = 256 * 1024 };
    var scratch = std.heap.ArenaAllocator.init(budget.allocator());
    defer scratch.deinit();
    const selected = compileRange(scratch.allocator(), recipe, conditions) catch |err| {
        if (err == error.OutOfMemory and budget.isLocallyExhausted()) return .{};
        return err;
    };
    return .{ .lower = try a.dupe(u8, selected.lower), .upper = if (selected.upper) |upper| try a.dupe(u8, upper) else null };
}
fn compileRange(a: A, recipe: local.sql_aggregate_materialization.Recipe, conditions: []const local.sql_catalog.Condition) !Range {
    var prefix: std.ArrayList(u8) = .empty;
    defer prefix.deinit(a);
    for (recipe.keys) |key| {
        const equality = for (conditions) |c| {
            if (std.mem.eql(u8, c.column, key.path) and (c.op == .is_null or (c.op == .eq and !c.datum().sql_null)) and typed(key, c.datum())) break c;
        } else null;
        if (equality) |c| {
            local.storage_db_relational_index_keys.appendSqlScalar(a, &prefix, if (c.op == .is_null) .{} else c.datum(), false) catch |err| {
                if (err == error.InvalidRelationalIndexBound or err == error.InvalidColumnValue or err == error.RelationalIndexKeyTooLarge) return .{};
                return err;
            };
            continue;
        }
        var result: Range = .{ .lower = try a.dupe(u8, prefix.items), .upper = try successor(a, prefix.items) };
        for (conditions) |c| {
            if (!std.mem.eql(u8, key.path, c.column) or !typed(key, c.datum()) or c.datum().sql_null) continue;
            if (c.op != .gt and c.op != .gte and c.op != .lt and c.op != .lte) continue;
            var bound: std.ArrayList(u8) = .empty;
            try bound.appendSlice(a, prefix.items);
            local.storage_db_relational_index_keys.appendSqlScalar(a, &bound, c.datum(), false) catch |err| {
                if (err == error.InvalidRelationalIndexBound or err == error.InvalidColumnValue or err == error.RelationalIndexKeyTooLarge) continue;
                return err;
            };
            const bytes: ?[]const u8 = if (c.op == .gt or c.op == .lte) try successor(a, bound.items) else bound.items;
            if (c.op == .gt or c.op == .gte) {
                if (bytes) |b| {
                    if (std.mem.order(u8, b, result.lower) == .gt) result.lower = b;
                } else return .{ .lower = "x", .upper = "" };
            } else if (bytes) |b| if (result.upper == null or std.mem.order(u8, b, result.upper.?) == .lt) {
                result.upper = b;
            };
        }
        return result;
    }
    return .{ .lower = try a.dupe(u8, prefix.items), .upper = try successor(a, prefix.items) };
}

test "external lake aggregate key fences preserve typed ranges NULLs and truncated strings" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    const D = local.sql_scalar.Datum;
    const recipe: local.sql_aggregate_materialization.Recipe = .{ .keys = &.{ .{ .path = "category", .type = .string, .nullable = true }, .{ .path = "id", .type = .integer, .nullable = false } }, .inputs = &.{} };
    const selected = try range(out, recipe, &.{ .{ .column = "category", .op = .eq, .value = .{ .string = "a" } }, .{ .column = "id", .op = .gte, .value = .{ .integer = 9007199254740993 } }, .{ .column = "id", .op = .lt, .value = .{ .integer = 9007199254740995 } } });
    try std.testing.expect(selected.contains(try encode(out, &.{ D.json(.{ .string = "a" }), D.json(.{ .integer = 9007199254740993 }) })));
    try std.testing.expect(!selected.contains(try encode(out, &.{ D.json(.{ .string = "a" }), D.json(.{ .integer = 9007199254740995 }) })));
    try std.testing.expect(!selected.contains(try encode(out, &.{ D.json(.{ .string = "b" }), D.json(.{ .integer = 9007199254740993 }) })));
    const text = try out.alloc(u8, 512);
    @memset(text, 'x');
    const rows = [_]local.sql_operators.Row{.{ .keys = &.{D.json(.{ .string = text })}, .values = &.{}, .ordinal = 0 }};
    const bounds = (try rowBounds(out, &rows)).?;
    try bounds.validate();
    const full = try encode(out, rows[0].keys);
    try std.testing.expect((Range{ .lower = full, .upper = try successor(out, full) }).overlaps(bounds));
    const nil = (try rowBounds(out, &.{.{ .keys = &.{.{}}, .values = &.{}, .ordinal = 0 }})).?;
    try nil.validate();
    try std.testing.expect(nil.upper == null);
    const floating: local.sql_aggregate_materialization.Recipe = .{ .keys = &.{.{ .path = "n", .type = .number, .nullable = false }}, .inputs = &.{} };
    try std.testing.expect((try rowBoundsTyped(out, &.{.{ .keys = &.{D.json(.{ .integer = 1 })}, .values = &.{}, .ordinal = 0 }}, floating)) == null);
}
