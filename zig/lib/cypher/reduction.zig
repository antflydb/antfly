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

//! Scope-local physical aggregate and grouping-expression slots.
const std = @import("std");
const ast = @import("ast.zig");
const expressions = @import("expressions.zig");
const A = std.mem.Allocator;

pub fn repeatable(expression: *const ast.Expr) bool {
    return expressions.rowLocal(expression, struct {
        pub fn permits(_: @This(), _: []const u8) bool {
            return true;
        }
    }{});
}
fn hashExpression(hasher: *std.hash.Wyhash, expression: *const ast.Expr) void {
    std.hash.autoHash(hasher, std.meta.activeTag(expression.value));
    switch (expression.value) {
        .exists, .pattern_comprehension => std.hash.autoHash(hasher, @intFromPtr(expression)),
        inline else => |payload| hashValue(hasher, payload),
    }
}
fn hashValue(hasher: *std.hash.Wyhash, value: anytype) void {
    const T = @TypeOf(value);
    if (T == *const ast.Expr) return hashExpression(hasher, value);
    if (T == ast.Function) {
        std.hash.autoHash(hasher, value.name.len);
        for (value.name) |byte| hasher.update(&.{std.ascii.toLower(byte)});
        hashValue(hasher, value.args);
        hashValue(hasher, value.distinct);
        hashValue(hasher, value.star);
        return;
    }
    switch (@typeInfo(T)) {
        .pointer => |p| {
            if (p.size == .slice) {
                std.hash.autoHash(hasher, value.len);
                if (p.child == u8) hasher.update(value) else for (value) |item| hashValue(hasher, item);
            } else @compileError("unsupported expression pointer");
        },
        .@"struct" => |s| inline for (s.field_names) |name| hashValue(hasher, @field(value, name)),
        .@"union" => {
            std.hash.autoHash(hasher, std.meta.activeTag(value));
            switch (value) {
                inline else => |payload| hashValue(hasher, payload),
            }
        },
        .optional => {
            std.hash.autoHash(hasher, value != null);
            if (value) |payload| hashValue(hasher, payload);
        },
        .float => std.hash.autoHash(hasher, @as(@Int(.unsigned, @bitSizeOf(T)), @bitCast(if (value == 0) @as(T, 0) else value))),
        .void => {},
        else => std.hash.autoHash(hasher, value),
    }
}
fn hash(expression: *const ast.Expr) u64 {
    var hasher = std.hash.Wyhash.init(0);
    hashExpression(&hasher, expression);
    return hasher.final();
}
/// Hashes select candidates; structural equality always verifies collisions.
/// Nonrepeatable expressions are identified only by their original pointer.
pub const Index = struct {
    const Entry = struct { expression: *const ast.Expr, slot: usize, next: ?usize };
    entries: std.ArrayList(Entry) = .empty,
    heads: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    pointers: std.AutoHashMapUnmanaged(*const ast.Expr, usize) = .empty,
    pub fn find(self: *const Index, expression: *const ast.Expr) ?usize {
        if (self.pointers.get(expression)) |slot| return slot;
        if (!repeatable(expression)) return null;
        var next = self.heads.get(hash(expression));
        while (next) |position| {
            const entry = self.entries.items[position];
            if (expressions.same(entry.expression, expression)) return entry.slot;
            next = entry.next;
        }
        return null;
    }
    pub fn intern(self: *Index, a: A, expression: *const ast.Expr, slot: usize) !usize {
        if (self.find(expression)) |prior| {
            try self.pointers.put(a, expression, prior);
            return prior;
        }
        try self.pointers.put(a, expression, slot);
        if (repeatable(expression)) {
            const key = hash(expression);
            const position = self.entries.items.len;
            try self.entries.append(a, .{ .expression = expression, .slot = slot, .next = self.heads.get(key) });
            try self.heads.put(a, key, position);
        }
        return slot;
    }
};
pub const Calls = struct {
    items: []const *const ast.Expr = &.{},
    storage: std.ArrayList(*const ast.Expr) = .empty,
    index: Index = .{},
    distinct_inputs: Index = .{},
    distinct_leaders: std.ArrayList(usize) = .empty,
    pub fn append(self: *Calls, a: A, expression: *const ast.Expr) !void {
        const slot = try self.index.intern(a, expression, self.items.len);
        if (slot == self.items.len) {
            const call = expression.value.call;
            const shared = call.distinct and call.args.len == 1 and repeatable(call.args[0]) and
                !std.ascii.eqlIgnoreCase(call.name, "min") and !std.ascii.eqlIgnoreCase(call.name, "max");
            const leader = if (shared) try self.distinct_inputs.intern(a, call.args[0], slot) else slot;
            try self.distinct_leaders.append(a, leader);
            try self.storage.append(a, expression);
            self.items = self.storage.items;
        }
    }
};
/// A scalar cardinality consumer does not require a collection buffer. Keep
/// the original source span (including its public column name), argument and
/// DISTINCT flag. This rewrite stays at the original evaluation site: it does
/// not hoist lazy CASE branches or cross correlated aggregate scopes.
fn cardinality(expression: *const ast.Expr) ?*const ast.Expr {
    if (expression.value != .call) return null;
    const call = expression.value.call;
    if (!std.ascii.eqlIgnoreCase(call.name, "size") or call.distinct or call.star or call.args.len != 1) return null;
    const input = call.args[0];
    if (input.value != .call) return null;
    const aggregate = input.value.call;
    if (!std.ascii.eqlIgnoreCase(aggregate.name, "collect") or aggregate.star or aggregate.args.len != 1) return null;
    return input;
}
const Cardinality = struct {
    a: A,
    source: []const u8,
    rewrites: ?*const @import("alias_cardinality.zig").Rewrites = null,
    /// A full consumer in the same projection still needs the physical collect
    /// slot. Leave matching size consumers on that shared slot rather than
    /// adding another argument evaluation and reducer.
    fn fullCollections(self: *@This(), value: anytype, full: *Index) anyerror!void {
        const T = @TypeOf(value);
        if (T == *const ast.Expr) {
            if (cardinality(value) != null) return;
            if (value.value == .call and std.ascii.eqlIgnoreCase(value.value.call.name, "collect")) {
                _ = try full.intern(self.a, value, full.entries.items.len);
                return;
            }
            switch (value.value) {
                .exists, .pattern_comprehension => return,
                inline else => |payload| try self.fullCollections(payload, full),
            }
            return;
        }
        switch (@typeInfo(T)) {
            .pointer => |p| if (p.size == .slice and p.child != u8) {
                for (value) |child| try self.fullCollections(child, full);
            },
            .@"struct" => |info| inline for (info.field_names) |name| try self.fullCollections(@field(value, name), full),
            .@"union" => switch (value) {
                inline else => |payload| try self.fullCollections(payload, full),
            },
            .optional => if (value) |child| try self.fullCollections(child, full),
            else => {},
        }
    }
    fn lower(self: *@This(), value: anytype, full: ?*const Index) anyerror!@TypeOf(value) {
        const T = @TypeOf(value);
        if (T == *const ast.Expr) {
            var expression = if (self.rewrites) |rewrites| rewrites.get(value) orelse value.* else value.*;
            if (cardinality(value)) |input| {
                if (full == null or full.?.find(input) == null) {
                    expression.value = .{ .call = input.value.call };
                    expression.value.call.name = "count";
                }
            }
            const nested = expression.value == .exists or expression.value == .pattern_comprehension;
            expression.value = try self.lower(expression.value, if (nested) null else full);
            if (std.meta.eql(expression, value.*)) return value;
            const result = try self.a.create(ast.Expr);
            result.* = expression;
            return result;
        }
        switch (@typeInfo(T)) {
            .pointer => |p| {
                if (p.size == .slice and p.child != u8) {
                    var result: ?[]p.child = null;
                    for (value, 0..) |child, i| {
                        const next = try self.lower(child, full);
                        if (result == null and !std.meta.eql(child, next)) result = try self.a.dupe(p.child, value);
                        if (result) |items| items[i] = next;
                    }
                    return result orelse value;
                }
                return value;
            },
            .@"struct" => |info| {
                var collections: Index = .{};
                const scope = if (T == ast.Projection) scope: {
                    try self.fullCollections(value, &collections);
                    break :scope &collections;
                } else full;
                var result = value;
                inline for (info.field_names) |name| @field(result, name) = try self.lower(@field(value, name), scope);
                // Replacing SIZE(alias) with a scalar binding must preserve the
                // public implicit column name rather than exposing that alias.
                if (T == ast.Item) if (result.alias == null and value.expression.value != .variable and result.expression.value == .variable) {
                    result.alias = try expressions.outputName(self.a, self.source, value.expression);
                };
                return result;
            },
            .@"union" => return switch (value) {
                inline else => |payload, tag| @unionInit(T, @tagName(tag), try self.lower(payload, full)),
            },
            .optional => return if (value) |child| try self.lower(child, full) else null,
            else => return value,
        }
    }
};
pub fn lowerCardinality(a: A, query: ast.Query) !ast.Query {
    var lower: Cardinality = .{ .a = a, .source = query.source };
    const direct = try lower.lower(query, null);
    var rewrites: @import("alias_cardinality.zig").Rewrites = .empty;
    try @import("alias_cardinality.zig").plan(a, query.source, direct, &rewrites);
    if (rewrites.count() == 0) return direct;
    lower.rewrites = &rewrites;
    return lower.lower(direct, null);
}

/// Compile pointer lookups once, instead of structurally comparing every key
/// on every projected row. Nested graph scopes own independent reductions.
pub fn keyUses(a: A, keys: *const Index, value: anytype, uses: *std.AutoHashMapUnmanaged(*const ast.Expr, usize)) anyerror!void {
    const T = @TypeOf(value);
    if (T == *const ast.Expr) {
        if (value.value == .call and expressions.aggregateName(value.value.call.name)) return;
        if (keys.find(value)) |slot| try uses.put(a, value, slot);
        switch (value.value) {
            .exists, .pattern_comprehension => return,
            inline else => |payload| try keyUses(a, keys, payload, uses),
        }
        return;
    }
    switch (@typeInfo(T)) {
        .pointer => |p| {
            if (p.size == .slice and p.child != u8) for (value) |child| try keyUses(a, keys, child, uses);
        },
        .@"struct" => |s| inline for (s.field_names) |name| try keyUses(a, keys, @field(value, name), uses),
        .@"union" => switch (value) {
            inline else => |payload| try keyUses(a, keys, payload, uses),
        },
        .optional => if (value) |payload| try keyUses(a, keys, payload, uses),
        else => {},
    }
}

fn slotScenario(backing: A) !void {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    const compiler = @import("compiler.zig");
    var diagnostic: compiler.Diagnostic = .{};
    var compiled = try compiler.compile(a, "RETURN count(x) AS a,COUNT(x) AS b,count(DISTINCT x) AS c,sum(0.0) AS d,SUM(-0.0) AS e,collect([x IN [1] | x+1]) AS f,COLLECT([x IN [1] | x+1]) AS g", .{}, &diagnostic);
    defer compiled.deinit();
    var calls: Calls = .{};
    for (compiled.query.clauses[0].operation.project.value.items) |item| try calls.append(a, item.expression);
    // -0.0 is a unary node, not the same tree as the literal 0.0.
    try std.testing.expectEqual(@as(usize, 5), calls.items.len);
    const items = compiled.query.clauses[0].operation.project.value.items;
    try std.testing.expectEqual(calls.index.pointers.get(items[0].expression), calls.index.pointers.get(items[1].expression));
    try std.testing.expect(calls.index.pointers.get(items[0].expression) != calls.index.pointers.get(items[2].expression));
    try std.testing.expectEqual(calls.index.pointers.get(items[5].expression), calls.index.pointers.get(items[6].expression));
    var positive: ast.Expr = .{ .span = .{}, .value = .{ .literal = .{ .number = 0.0 } } };
    var negative: ast.Expr = .{ .span = .{}, .value = .{ .literal = .{ .number = -0.0 } } };
    try std.testing.expectEqual(hash(&positive), hash(&negative));
    var impure: ast.Expr = .{ .span = .{}, .value = .{ .exists = &.{} } };
    var impure_copy = impure;
    var index: Index = .{};
    _ = try index.intern(a, &impure, 0);
    try std.testing.expect(index.find(&impure_copy) == null);
    // Unknown/future scalar functions do not license sharing either.
    var unknown: ast.Expr = .{ .span = .{}, .value = .{ .call = .{ .name = "future_random", .args = &.{} } } };
    try std.testing.expect(!repeatable(&unknown));
}
test "scope-local slots share repeatable calls and preserve distinct and effect boundaries" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, slotScenario, .{});
}

test "cardinality planning retains source names shared full consumers and unchanged trees" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const compiler = @import("compiler.zig");
    var diagnostic: compiler.Diagnostic = .{};
    var compiled = try compiler.compile(a, "UNWIND [1] AS x RETURN size(collect(x)),size(COLLECT(DISTINCT x)),collect(DISTINCT x)", .{}, &diagnostic);
    defer compiled.deinit();
    const lowered = try lowerCardinality(a, compiled.query);
    const original = compiled.query.clauses[1].operation.project.value.items;
    const items = lowered.clauses[1].operation.project.value.items;
    try std.testing.expectEqualStrings("count", items[0].expression.value.call.name);
    try std.testing.expectEqual(original[0].expression.span, items[0].expression.span);
    try std.testing.expectEqual(original[1].expression, items[1].expression);
    try std.testing.expectEqual(original[2].expression, items[2].expression);
    try std.testing.expectEqual(compiled.query.clauses[0].operation.unwind.expression, lowered.clauses[0].operation.unwind.expression);
    // Lowering never mutates the compiled public schema or cached AST.
    try std.testing.expectEqualStrings("size", original[0].expression.value.call.name);
}
