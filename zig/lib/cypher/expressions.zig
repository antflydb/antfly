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

const std = @import("std");
const ast = @import("ast.zig");

/// Nested graph patterns can bind an alias that is absent from a partial row.
/// Evaluate these only after all of the containing MATCH aliases are bound.
pub fn containsPattern(expr: *const ast.Expr) bool {
    return switch (expr.value) {
        .exists, .pattern_comprehension => true,
        .literal, .variable, .parameter => false,
        .property => |p| containsPattern(p.base),
        .index => |i| containsPattern(i.base) or containsPattern(i.index),
        .slice => |s| containsPattern(s.base) or optionalPattern(s.start) or optionalPattern(s.end),
        .unary => |u| containsPattern(u.operand),
        .binary => |b| containsPattern(b.left) or containsPattern(b.right),
        .comparison => |c| anyPattern(c.operands),
        .call => |c| anyPattern(c.args),
        .list => |items| anyPattern(items),
        .comprehension => |c| containsPattern(c.input) or optionalPattern(c.predicate) or optionalPattern(c.projection),
        .list_predicate => |c| containsPattern(c.input) or containsPattern(c.predicate),
        .reduce => |r| containsPattern(r.initial) or containsPattern(r.input) or containsPattern(r.projection),
        .map => |entries| blk: {
            for (entries) |entry| if (containsPattern(entry.value)) break :blk true;
            break :blk false;
        },
        .map_projection => |p| blk: {
            if (containsPattern(p.base)) break :blk true;
            for (p.items) |item| if (item == .entry and containsPattern(item.entry.value)) break :blk true;
            break :blk false;
        },
        .case_when => |c| blk: {
            if (optionalPattern(c.operand) or optionalPattern(c.otherwise)) break :blk true;
            for (c.branches) |branch| if (containsPattern(branch.condition) or containsPattern(branch.value)) break :blk true;
            break :blk false;
        },
    };
}
fn optionalPattern(expr: ?*const ast.Expr) bool {
    return if (expr) |e| containsPattern(e) else false;
}
fn anyPattern(items: []const *const ast.Expr) bool {
    for (items) |item| if (containsPattern(item)) return true;
    return false;
}
pub fn aggregateName(name: []const u8) bool {
    for ([_][]const u8{ "count", "sum", "avg", "min", "max", "collect" }) |n| if (std.ascii.eqlIgnoreCase(name, n)) return true;
    return false;
}
pub fn aggregates(expression: *const ast.Expr) bool {
    return switch (expression.value) {
        .call => |c| aggregateName(c.name) or blk: {
            for (c.args) |arg| if (aggregates(arg)) break :blk true;
            break :blk false;
        },
        .comprehension => |c| aggregates(c.input),
        .list_predicate => |c| aggregates(c.input),
        .reduce => |c| aggregates(c.initial) or aggregates(c.input),
        .map_projection => |p| aggregates(p.base) or blk: {
            for (p.items) |item| if (item == .entry and aggregates(item.entry.value)) break :blk true;
            break :blk false;
        },
        .binary => |b| aggregates(b.left) or aggregates(b.right),
        .comparison => |c| blk: {
            for (c.operands) |operand| if (aggregates(operand)) break :blk true;
            break :blk false;
        },
        .unary => |u| aggregates(u.operand),
        .property => |p| aggregates(p.base),
        .index => |i| aggregates(i.base) or aggregates(i.index),
        .slice => |s| aggregates(s.base) or (if (s.start) |bound| aggregates(bound) else false) or (if (s.end) |bound| aggregates(bound) else false),
        .list => |items| blk: {
            for (items) |item| if (aggregates(item)) break :blk true;
            break :blk false;
        },
        .map => |items| blk: {
            for (items) |item| if (aggregates(item.value)) break :blk true;
            break :blk false;
        },
        .case_when => |c| blk: {
            if (c.operand) |operand| if (aggregates(operand)) break :blk true;
            for (c.branches) |b| if (aggregates(b.condition) or aggregates(b.value)) break :blk true;
            if (c.otherwise) |other| if (aggregates(other)) break :blk true;
            break :blk false;
        },
        else => false,
    };
}

/// Structural matching of projected grouping expressions does not depend on
/// source whitespace or allocations in separately parsed expressions.
pub fn same(a: *const ast.Expr, b: *const ast.Expr) bool {
    if (std.meta.activeTag(a.value) != std.meta.activeTag(b.value)) return false;
    return switch (a.value) {
        .variable => |x| std.mem.eql(u8, x, b.value.variable),
        .parameter => |x| std.mem.eql(u8, x, b.value.parameter),
        .literal => |x| blk: {
            const y = b.value.literal;
            if (std.meta.activeTag(x) != std.meta.activeTag(y)) break :blk false;
            break :blk switch (x) {
                .null => true,
                .boolean => x.boolean == y.boolean,
                .integer => x.integer == y.integer,
                .number => x.number == y.number,
                .string => std.mem.eql(u8, x.string, y.string),
            };
        },
        .property => |x| std.mem.eql(u8, x.name, b.value.property.name) and same(x.base, b.value.property.base),
        .index => |x| same(x.base, b.value.index.base) and same(x.index, b.value.index.index),
        .slice => |x| same(x.base, b.value.slice.base) and sameOptional(x.start, b.value.slice.start) and sameOptional(x.end, b.value.slice.end),
        .unary => |x| x.op == b.value.unary.op and same(x.operand, b.value.unary.operand),
        .binary => |x| x.op == b.value.binary.op and same(x.left, b.value.binary.left) and same(x.right, b.value.binary.right),
        .comparison => |x| blk: {
            const y = b.value.comparison;
            if (!std.mem.eql(ast.Binary, x.operators, y.operators) or x.operands.len != y.operands.len) break :blk false;
            for (x.operands, y.operands) |left, right| if (!same(left, right)) break :blk false;
            break :blk true;
        },
        .call => |x| blk: {
            const y = b.value.call;
            if (!std.ascii.eqlIgnoreCase(x.name, y.name) or x.star != y.star or x.distinct != y.distinct or x.args.len != y.args.len) break :blk false;
            for (x.args, y.args) |left, right| if (!same(left, right)) break :blk false;
            break :blk true;
        },
        .list => |x| blk: {
            if (x.len != b.value.list.len) break :blk false;
            for (x, b.value.list) |left, right| if (!same(left, right)) break :blk false;
            break :blk true;
        },
        .map => |x| blk: {
            if (x.len != b.value.map.len) break :blk false;
            for (x, b.value.map) |left, right| if (!std.mem.eql(u8, left.name, right.name) or !same(left.value, right.value)) break :blk false;
            break :blk true;
        },
        .map_projection => |x| blk: {
            const y = b.value.map_projection;
            if (!same(x.base, y.base) or x.items.len != y.items.len) break :blk false;
            for (x.items, y.items) |left, right| {
                if (std.meta.activeTag(left) != std.meta.activeTag(right)) break :blk false;
                const matches = switch (left) {
                    .all => true,
                    .property => |name| std.mem.eql(u8, name, right.property),
                    .variable => |name| std.mem.eql(u8, name, right.variable),
                    .entry => |entry| std.mem.eql(u8, entry.name, right.entry.name) and same(entry.value, right.entry.value),
                };
                if (!matches) break :blk false;
            }
            break :blk true;
        },
        .comprehension => |x| blk: {
            const y = b.value.comprehension;
            break :blk std.mem.eql(u8, x.alias, y.alias) and same(x.input, y.input) and sameOptional(x.predicate, y.predicate) and sameOptional(x.projection, y.projection);
        },
        .pattern_comprehension => |x| blk: {
            const y = b.value.pattern_comprehension;
            break :blk samePath(x.path, y.path) and sameOptional(x.predicate, y.predicate) and same(x.projection, y.projection);
        },
        .list_predicate => |x| blk: {
            const y = b.value.list_predicate;
            break :blk x.kind == y.kind and std.mem.eql(u8, x.alias, y.alias) and same(x.input, y.input) and same(x.predicate, y.predicate);
        },
        .reduce => |x| blk: {
            const y = b.value.reduce;
            break :blk std.mem.eql(u8, x.accumulator, y.accumulator) and std.mem.eql(u8, x.alias, y.alias) and same(x.initial, y.initial) and same(x.input, y.input) and same(x.projection, y.projection);
        },
        .case_when => |x| blk: {
            const y = b.value.case_when;
            if (!sameOptional(x.operand, y.operand) or !sameOptional(x.otherwise, y.otherwise) or x.branches.len != y.branches.len) break :blk false;
            for (x.branches, y.branches) |left, right| if (!same(left.condition, right.condition) or !same(left.value, right.value)) break :blk false;
            break :blk true;
        },
        else => a == b,
    };
}

fn sameOptional(a: ?*const ast.Expr, b: ?*const ast.Expr) bool {
    if (a) |left| return if (b) |right| same(left, right) else false;
    return b == null;
}

fn sameName(a: ?[]const u8, b: ?[]const u8) bool {
    return if (a) |left| if (b) |right| std.mem.eql(u8, left, right) else false else b == null;
}
fn sameNames(a: []const []const u8, b: []const []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| if (!std.mem.eql(u8, left, right)) return false;
    return true;
}
fn sameEntries(a: []const ast.Entry, b: []const ast.Entry) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| if (!std.mem.eql(u8, left.name, right.name) or !same(left.value, right.value)) return false;
    return true;
}
fn samePath(a: ast.Path, b: ast.Path) bool {
    if (!sameName(a.alias, b.alias) or a.selector != b.selector or a.nodes.len != b.nodes.len or a.edges.len != b.edges.len) return false;
    for (a.nodes, b.nodes) |left, right| if (!sameName(left.alias, right.alias) or !sameNames(left.labels, right.labels) or !sameEntries(left.properties, right.properties)) return false;
    for (a.edges, b.edges) |left, right| {
        if (!sameName(left.alias, right.alias) or !sameNames(left.types, right.types) or !sameEntries(left.properties, right.properties) or left.direction != right.direction or left.min_hops != right.min_hops or left.max_hops != right.max_hops or left.variable_length != right.variable_length) return false;
    }
    return true;
}

/// Aggregated expressions may reference projected grouping keys, literals and
/// parameters, but never select an arbitrary ungrouped row's values.
pub fn validateGrouping(expr: *const ast.Expr, items: []const ast.Item, aliases: bool) error{CypherInvalidGrouping}!void {
    try grouping(expr, items, aliases, null);
}
const Local = struct { name: []const u8, parent: ?*const Local };
fn grouping(expr: *const ast.Expr, items: []const ast.Item, aliases: bool, locals: ?*const Local) error{CypherInvalidGrouping}!void {
    if (expr.value == .variable) {
        var current = locals;
        while (current) |local| : (current = local.parent) {
            if (std.mem.eql(u8, expr.value.variable, local.name)) return;
        }
    }
    for (items) |item| {
        if (!aggregates(item.expression) and same(expr, item.expression)) return;
        if (aliases and expr.value == .variable and item.alias != null and std.mem.eql(u8, expr.value.variable, item.alias.?)) return;
    }
    switch (expr.value) {
        .literal, .parameter => {},
        .comprehension => |c| {
            try grouping(c.input, items, aliases, locals);
            const local: Local = .{ .name = c.alias, .parent = locals };
            if (c.predicate) |predicate| try grouping(predicate, items, aliases, &local);
            if (c.projection) |projection| try grouping(projection, items, aliases, &local);
        },
        .list_predicate => |c| {
            try grouping(c.input, items, aliases, locals);
            const local: Local = .{ .name = c.alias, .parent = locals };
            try grouping(c.predicate, items, aliases, &local);
        },
        .reduce => |c| {
            try grouping(c.initial, items, aliases, locals);
            try grouping(c.input, items, aliases, locals);
            const accumulator: Local = .{ .name = c.accumulator, .parent = locals };
            const local: Local = .{ .name = c.alias, .parent = &accumulator };
            try grouping(c.projection, items, aliases, &local);
        },
        .variable, .exists, .pattern_comprehension => return error.CypherInvalidGrouping,
        .property => |x| try grouping(x.base, items, aliases, locals),
        .index => |x| {
            try grouping(x.base, items, aliases, locals);
            try grouping(x.index, items, aliases, locals);
        },
        .slice => |s| {
            try grouping(s.base, items, aliases, locals);
            if (s.start) |bound| try grouping(bound, items, aliases, locals);
            if (s.end) |bound| try grouping(bound, items, aliases, locals);
        },
        .unary => |x| try grouping(x.operand, items, aliases, locals),
        .binary => |x| {
            try grouping(x.left, items, aliases, locals);
            try grouping(x.right, items, aliases, locals);
        },
        .comparison => |c| for (c.operands) |operand| try grouping(operand, items, aliases, locals),
        .call => |x| {
            if (aggregateName(x.name)) return;
            for (x.args) |arg| try grouping(arg, items, aliases, locals);
        },
        .list => |x| for (x) |item| try grouping(item, items, aliases, locals),
        .map => |x| for (x) |item| try grouping(item.value, items, aliases, locals),
        .map_projection => |p| {
            try grouping(p.base, items, aliases, locals);
            for (p.items) |item| switch (item) {
                .entry => |entry| try grouping(entry.value, items, aliases, locals),
                .variable => |name| {
                    const variable: ast.Expr = .{ .span = .{}, .value = .{ .variable = name } };
                    try grouping(&variable, items, aliases, locals);
                },
                .property, .all => {},
            };
        },
        .case_when => |x| {
            if (x.operand) |operand| try grouping(operand, items, aliases, locals);
            for (x.branches) |branch| {
                try grouping(branch.condition, items, aliases, locals);
                try grouping(branch.value, items, aliases, locals);
            }
            if (x.otherwise) |other| try grouping(other, items, aliases, locals);
        },
    }
}
