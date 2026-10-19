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

//! Execution-only path demands. Public schemas and full path identities retain
//! their original AST; private scalar bindings never enter wildcard output.
const std = @import("std");
const ast = @import("ast.zig");
const A = std.mem.Allocator;
const Source = struct {
    path: ?*ast.Path = null,
    optional: bool = false,
    edge: ?*ast.Edge = null,
    presence: ?[]const u8 = null,
    length: ?[]const u8 = null,
    first: ?[]const u8 = null,
    last: ?[]const u8 = null,
};
const Binding = struct { name: []const u8, source: ?*Source = null };
const Carry = struct { projection: *ast.Projection, sources: []const *Source };
const Lower = struct {
    a: A,
    source: []const u8,
    next: usize = 0,
    changed: bool = false,
    used: std.StringHashMapUnmanaged(void) = .empty,
    carries: std.ArrayList(Carry) = .empty,
    scopes: std.ArrayList(struct { clauses: []const ast.Clause, caller: ?[]const ast.Clause = null }) = .empty,
    fn privateName(self: *@This()) ![]const u8 {
        while (true) {
            self.next += 1;
            const candidate = try std.fmt.allocPrint(self.a, "\x00path:{d}", .{self.next});
            const entry = try self.used.getOrPut(self.a, candidate);
            if (!entry.found_existing) return candidate;
        }
    }
    fn reserve(self: *@This(), value: anytype) anyerror!void {
        const T = @TypeOf(value);
        if (T == []const u8) {
            try self.used.put(self.a, value, {});
            return;
        }
        switch (@typeInfo(T)) {
            .pointer => |p| {
                if (p.size == .one and p.child == ast.Expr) try self.reserve(value.*);
                if (p.size == .slice and p.child != u8) for (value) |child| try self.reserve(child);
            },
            .@"struct" => |info| inline for (info.field_names) |field_name| {
                try self.reserve(@field(value, field_name));
            },
            .@"union" => switch (value) {
                inline else => |child| try self.reserve(child),
            },
            .optional => if (value) |child| try self.reserve(child),
            else => {},
        }
    }
    fn variable(self: *@This(), name: []const u8, span: ast.Span) !*const ast.Expr {
        const e = try self.a.create(ast.Expr);
        e.* = .{ .span = span, .value = .{ .variable = name } };
        return e;
    }
    fn field(self: *@This(), value: *Source, comptime which: []const u8, span: ast.Span) !*const ast.Expr {
        const slot = &@field(value, which);
        if (slot.* == null) slot.* = try self.privateName();
        if (value.path) |path| @field(path.execution_summary, which) = slot.* else value.edge.?.execution_length = slot.*;
        self.changed = true;
        return self.variable(slot.*.?, span);
    }
    fn find(scope: []const Binding, name: []const u8) ?*Source {
        for (scope) |entry| if (std.mem.eql(u8, entry.name, name)) return entry.source;
        return null;
    }
    fn contains(scope: []const Binding, name: []const u8) bool {
        for (scope) |entry| if (std.mem.eql(u8, entry.name, name)) return true;
        return false;
    }
    fn reference(scope: []const Binding, expr: *const ast.Expr) ?*Source {
        return if (expr.value == .variable) find(scope, expr.value.variable) else null;
    }
    fn called(expr: *const ast.Expr, name: []const u8) ?*const ast.Expr {
        if (expr.value != .call or !std.ascii.eqlIgnoreCase(expr.value.call.name, name) or expr.value.call.args.len != 1) return null;
        return expr.value.call.args[0];
    }
    fn endpoint(self: *@This(), scope: []const Binding, expr: *const ast.Expr, last: bool, span: ast.Span) !?*const ast.Expr {
        const path = called(expr, "nodes") orelse return null;
        const ref = reference(scope, path) orelse return null;
        if (ref.path == null) return null;
        return if (last) try self.field(ref, "last", span) else try self.field(ref, "first", span);
    }
    fn expression(self: *@This(), original: *const ast.Expr, scope: []const Binding) anyerror!*const ast.Expr {
        if (original.value == .call) {
            const call = original.value.call;
            if (call.args.len == 1) {
                const arg = call.args[0];
                if (reference(scope, arg)) |ref| {
                    if (ref.path != null and std.ascii.eqlIgnoreCase(call.name, "count") and !call.distinct) {
                        if (!ref.optional) {
                            const result = try self.a.create(ast.Expr);
                            result.* = original.*;
                            result.value.call.star = true;
                            result.value.call.args = &.{};
                            self.changed = true;
                            return result;
                        }
                        return self.replaceArgument(original, try self.field(ref, "presence", arg.span));
                    }
                    if ((ref.path != null and std.ascii.eqlIgnoreCase(call.name, "length")) or (ref.edge != null and std.ascii.eqlIgnoreCase(call.name, "size")))
                        return self.field(ref, "length", original.span);
                }
                if (std.ascii.eqlIgnoreCase(call.name, "head") or std.ascii.eqlIgnoreCase(call.name, "last"))
                    if (try self.endpoint(scope, arg, std.ascii.eqlIgnoreCase(call.name, "last"), original.span)) |result| return result;
                if (std.ascii.eqlIgnoreCase(call.name, "size")) {
                    if (called(arg, "relationships")) |path| if (reference(scope, path)) |ref| {
                        if (ref.path != null) return self.field(ref, "length", original.span);
                    };
                    if (called(arg, "nodes")) |path| if (reference(scope, path)) |ref| {
                        if (ref.path != null) {
                            const one = try self.a.create(ast.Expr);
                            one.* = .{ .span = original.span, .value = .{ .literal = .{ .integer = 1 } } };
                            const result = try self.a.create(ast.Expr);
                            result.* = .{ .span = original.span, .value = .{ .binary = .{ .op = .add, .left = try self.field(ref, "length", original.span), .right = one } } };
                            return result;
                        }
                    };
                }
            }
        }
        if (original.value == .index) {
            const index = original.value.index;
            const offset: ?i64 = if (index.index.value == .literal and index.index.value.literal == .integer) index.index.value.literal.integer else if (index.index.value == .unary and index.index.value.unary.op == .negative and index.index.value.unary.operand.value == .literal and index.index.value.unary.operand.value.literal == .integer) (if (index.index.value.unary.operand.value.literal.integer == 1) @as(i64, -1) else if (index.index.value.unary.operand.value.literal.integer == 0) @as(i64, 0) else null) else null;
            if (offset == 0 or offset == -1) if (try self.endpoint(scope, index.base, offset == -1, original.span)) |result| return result;
        }
        const result = try self.a.create(ast.Expr);
        result.* = original.*;
        switch (original.value) {
            .exists => |inner| result.value.exists = (try self.clauses(inner, scope, false)).clauses,
            .pattern_comprehension => |c| {
                // Use the same lexical scope rules as MATCH/RETURN. Correlated
                // sources retain their lineage; locally introduced aliases do
                // not escape the expression.
                var local: std.ArrayList(Binding) = .empty;
                for (scope) |entry| {
                    if (c.path.alias) |alias| if (std.mem.eql(u8, alias, entry.name)) continue;
                    try local.append(self.a, entry);
                }
                const child = try self.clauses(&.{
                    .{ .span = original.span, .operation = .{ .match = .{ .paths = &.{c.path}, .predicate = c.predicate } } },
                    .{ .span = original.span, .operation = .{ .project = .{ .final = true, .value = .{ .items = &.{.{ .expression = c.projection }} } } } },
                }, local.items, true);
                result.value.pattern_comprehension = .{
                    .path = child.clauses[0].operation.match.paths[0],
                    .predicate = child.clauses[0].operation.match.predicate,
                    .projection = child.clauses[1].operation.project.value.items[0].expression,
                };
            },
            .comprehension => |c| {
                var local: std.ArrayList(Binding) = .empty;
                try local.appendSlice(self.a, scope);
                try self.bind(&local, c.alias, null);
                result.value.comprehension.input = try self.expression(c.input, scope);
                result.value.comprehension.predicate = if (c.predicate) |p| try self.expression(p, local.items) else null;
                result.value.comprehension.projection = if (c.projection) |p| try self.expression(p, local.items) else null;
            },
            .list_predicate => |c| {
                var local: std.ArrayList(Binding) = .empty;
                try local.appendSlice(self.a, scope);
                try self.bind(&local, c.alias, null);
                result.value.list_predicate.input = try self.expression(c.input, scope);
                result.value.list_predicate.predicate = try self.expression(c.predicate, local.items);
            },
            .reduce => |r| {
                var local: std.ArrayList(Binding) = .empty;
                try local.appendSlice(self.a, scope);
                try self.bind(&local, r.alias, null);
                try self.bind(&local, r.accumulator, null);
                result.value.reduce.initial = try self.expression(r.initial, scope);
                result.value.reduce.input = try self.expression(r.input, scope);
                result.value.reduce.projection = try self.expression(r.projection, local.items);
            },
            else => result.value = try self.tree(original.value, scope),
        }
        return result;
    }
    fn replaceArgument(self: *@This(), original: *const ast.Expr, arg: *const ast.Expr) !*const ast.Expr {
        const result = try self.a.create(ast.Expr);
        result.* = original.*;
        result.value.call.args = try self.a.dupe(*const ast.Expr, &.{arg});
        return result;
    }
    fn tree(self: *@This(), value: anytype, scope: []const Binding) anyerror!@TypeOf(value) {
        const T = @TypeOf(value);
        if (T == *const ast.Expr) return self.expression(value, scope);
        var result = value;
        switch (@typeInfo(T)) {
            .pointer => |p| if (p.size == .slice and p.child != u8) {
                const out = try self.a.alloc(p.child, value.len);
                for (value, out) |old, *new| new.* = try self.tree(old, scope);
                return out;
            },
            .@"struct" => |s| inline for (s.field_names) |name| {
                @field(result, name) = try self.tree(@field(value, name), scope);
            },
            .@"union" => switch (value) {
                inline else => |child, tag| return @unionInit(T, @tagName(tag), try self.tree(child, scope)),
            },
            .optional => if (value) |child| return try self.tree(child, scope),
            else => {},
        }
        return result;
    }
    fn bind(self: *@This(), scope: *std.ArrayList(Binding), name: []const u8, ref: ?*Source) !void {
        for (scope.items) |*entry| if (std.mem.eql(u8, entry.name, name)) {
            entry.source = ref;
            return;
        };
        try scope.append(self.a, .{ .name = name, .source = ref });
    }
    fn expressionName(self: *@This(), expr: *const ast.Expr) anyerror![]const u8 {
        if (expr.value == .map_projection and expr.value.map_projection.base.value == .variable) return expr.value.map_projection.base.value.variable;
        if (expr.value != .variable and expr.value != .property and expr.span.start <= expr.span.end and expr.span.end <= self.source.len) return self.source[expr.span.start..expr.span.end];
        return switch (expr.value) {
            .variable => |name| name,
            .property => |p| std.fmt.allocPrint(self.a, "{s}.{s}", .{ try self.expressionName(p.base), p.name }),
            .call => |c| std.fmt.allocPrint(self.a, "{s}({s})", .{ c.name, if (c.star) "*" else if (c.args.len == 1) try self.expressionName(c.args[0]) else "..." }),
            else => std.fmt.allocPrint(self.a, "expression@{}", .{expr.span.start}),
        };
    }
    fn itemName(self: *@This(), item: ast.Item) ![]const u8 {
        return item.alias orelse try self.expressionName(item.expression);
    }
    fn clauses(self: *@This(), old: []const ast.Clause, input: []const Binding, public: bool) anyerror!struct { clauses: []const ast.Clause, scope: []const Binding } {
        const out = try self.a.dupe(ast.Clause, old);
        var scope: std.ArrayList(Binding) = .empty;
        try scope.appendSlice(self.a, input);
        for (out, 0..) |*clause, clause_index| switch (clause.operation) {
            .match => |*matching| {
                const paths = try self.a.dupe(ast.Path, matching.paths);
                matching.paths = paths;
                for (paths) |*path| {
                    for (path.nodes) |node| if (node.alias) |alias| {
                        var found = false;
                        for (scope.items) |entry| if (std.mem.eql(u8, entry.name, alias)) {
                            found = true;
                            break;
                        };
                        if (!found) try self.bind(&scope, alias, null);
                    };
                    const edges = try self.a.dupe(ast.Edge, path.edges);
                    path.edges = edges;
                    for (edges) |*edge| if (edge.alias) |alias| {
                        if (contains(scope.items, alias)) continue;
                        const ref = if (edge.variable_length) try self.a.create(Source) else null;
                        if (ref) |source| source.* = .{ .edge = edge };
                        try self.bind(&scope, alias, ref);
                    };
                    if (path.alias) |alias| {
                        if (contains(scope.items, alias)) continue;
                        const ref = try self.a.create(Source);
                        ref.* = .{ .path = path, .optional = matching.optional };
                        try self.bind(&scope, alias, ref);
                    }
                }
                if (matching.predicate) |predicate| matching.predicate = try self.expression(predicate, scope.items);
            },
            .unwind => |*u| {
                u.expression = try self.expression(u.expression, scope.items);
                try self.bind(&scope, u.alias, null);
            },
            .subquery => |inner| {
                const child = try self.clauses(inner, scope.items, false);
                clause.operation.subquery = child.clauses;
                self.scopes.items[self.scopes.items.len - 1].caller = out[clause_index + 1 ..];
                if (inner.len != 0 and inner[inner.len - 1].operation == .project and inner[inner.len - 1].operation.project.final)
                    for (child.scope) |entry| try self.bind(&scope, entry.name, entry.source);
            },
            .project => |*step| {
                const projection = &step.value;
                var items: std.ArrayList(ast.Item) = .empty;
                var next: std.ArrayList(Binding) = .empty;
                if (projection.wildcard) for (scope.items) |entry| {
                    try items.append(self.a, .{ .expression = try self.variable(entry.name, clause.span), .alias = entry.name });
                    try self.bind(&next, entry.name, entry.source);
                };
                for (projection.items) |item| {
                    const name = try self.itemName(item);
                    try items.append(self.a, .{ .alias = name, .expression = try self.expression(item.expression, scope.items) });
                    try self.bind(&next, name, reference(scope.items, item.expression));
                }
                projection.items = items.items;
                projection.wildcard = false;
                var visible: std.ArrayList(Binding) = .empty;
                try visible.appendSlice(self.a, scope.items);
                for (next.items) |entry| try self.bind(&visible, entry.name, entry.source);
                if (projection.predicate) |predicate| projection.predicate = try self.expression(predicate, visible.items);
                const orders = try self.a.dupe(ast.Order, projection.order);
                for (orders) |*order| order.expression = try self.expression(order.expression, visible.items);
                projection.order = orders;
                // Carry summary slots through aliases without exposing them in
                // RETURN *. Final projection schemas remain the public schema.
                if (!step.final or !public) {
                    var refs: std.ArrayList(*Source) = .empty;
                    for (next.items) |entry| if (entry.source) |ref| {
                        if (std.mem.indexOfScalar(*Source, refs.items, ref) == null) try refs.append(self.a, ref);
                    };
                    try self.carries.append(self.a, .{ .projection = projection, .sources = refs.items });
                }
                scope = next;
            },
            else => {},
        };
        // Children are registered first, so their discarded imports disappear
        // before a parent computes its correlated dependencies.
        try self.scopes.append(self.a, .{ .clauses = out });
        return .{ .clauses = out, .scope = scope.items };
    }
};
fn eligible(value: anytype) bool {
    if (@TypeOf(value) == ast.Clause) switch (value.operation) {
        .match, .unwind, .project, .subquery => {},
        else => return false,
    };
    switch (@typeInfo(@TypeOf(value))) {
        .pointer => |p| {
            if (p.size == .one and p.child == ast.Expr) return eligible(value.*);
            if (p.size == .slice and p.child != u8) for (value) |child| {
                if (!eligible(child)) return false;
            };
        },
        .@"struct" => |s| inline for (s.field_names) |name| {
            if (!eligible(@field(value, name))) return false;
        },
        .@"union" => switch (value) {
            inline else => |child| return eligible(child),
        },
        .optional => if (value) |child| return eligible(child),
        else => {},
    }
    return true;
}
fn hasPath(value: anytype) bool {
    if (@TypeOf(value) == ast.Path) {
        if (value.alias != null) return true;
        for (value.edges) |edge| if (edge.alias != null and edge.variable_length) return true;
    }
    switch (@typeInfo(@TypeOf(value))) {
        .pointer => |p| {
            if (p.size == .one and p.child == ast.Expr) return hasPath(value.*);
            if (p.size == .slice and p.child != u8) for (value) |child| {
                if (hasPath(child)) return true;
            };
        },
        .@"struct" => |s| inline for (s.field_names) |name| {
            if (hasPath(@field(value, name))) return true;
        },
        .@"union" => switch (value) {
            inline else => |child| return hasPath(child),
        },
        .optional => if (value) |child| return hasPath(child),
        else => {},
    }
    return false;
}
pub fn lower(a: A, query: ast.Query) !ast.Query {
    if (!hasPath(query) or !eligible(query)) return query;
    var plan: Lower = .{ .a = a, .source = query.source };
    try plan.reserve(query);
    var result = query;
    result.clauses = (try plan.clauses(query.clauses, &.{}, true)).clauses;
    const arms = try a.dupe(ast.UnionArm, query.unions);
    for (arms) |*arm| arm.clauses = (try plan.clauses(arm.clauses, &.{}, true)).clauses;
    result.unions = arms;
    if (!plan.changed) return query;
    for (plan.carries.items) |carry| {
        var items: std.ArrayList(ast.Item) = .empty;
        try items.appendSlice(a, carry.projection.items);
        for (carry.sources) |ref| inline for (.{ "presence", "length", "first", "last" }) |field| if (@field(ref, field)) |name| {
            var found = false;
            for (items.items) |item| if (item.alias != null and std.mem.eql(u8, item.alias.?, name)) {
                found = true;
                break;
            };
            if (!found) try items.append(a, .{ .alias = name, .expression = try plan.variable(name, .{}) });
        };
        carry.projection.items = items.items;
    }
    // Iterate to a fixed point: discarded caller imports can make a private
    // CALL export dead, which can in turn release a child import. Only total
    // expressions are removed; aggregate keys, DISTINCT and errors stay live.
    var pruned = true;
    while (pruned) {
        pruned = false;
        for (plan.scopes.items) |scope| {
            const clauses = scope.clauses;
            var i = clauses.len;
            while (i != 0) {
                i -= 1;
                // The arrays are owned by this lowering pass. Public final items,
                // DISTINCT keys and potentially failing expressions remain intact.
                const clause = @constCast(&clauses[i]);
                if (clause.operation != .project) continue;
                if (clause.operation.project.final and scope.caller == null) continue;
                const projection = &clause.operation.project.value;
                if (projection.distinct) continue;
                const grouped = for (projection.items) |item| {
                    if (@import("expressions.zig").aggregates(item.expression)) break true;
                } else false;
                if (grouped) continue;
                var items: std.ArrayList(ast.Item) = .empty;
                for (projection.items) |item| {
                    const name = item.alias orelse try plan.expressionName(item.expression);
                    if (!total(item.expression) or references(.{ projection.predicate, projection.order, clauses[i + 1 ..], scope.caller }, name))
                        try items.append(a, item);
                }
                pruned = pruned or items.items.len != projection.items.len;
                projection.items = items.items;
            }
        }
    }
    return result;
}
fn total(expression: *const ast.Expr) bool {
    return switch (expression.value) {
        .literal, .variable => true,
        .list => |items| blk: {
            for (items) |item| if (!total(item)) break :blk false;
            break :blk true;
        },
        .map => |items| blk: {
            for (items) |item| if (!total(item.value)) break :blk false;
            break :blk true;
        },
        else => false,
    };
}

/// Execution metadata is a demand, not a binding declaration. Recompute it
/// after dead projections and downstream column selection have been removed.
/// Private names are collision-free across all lexical scopes.
pub fn references(value: anytype, name: []const u8) bool {
    const T = @TypeOf(value);
    // MATCH aliases can constrain an existing binding even without a variable
    // expression. Keeping them is essential when pruning WITH imports.
    if (T == ast.Path or T == ast.Node or T == ast.Edge) {
        if (value.alias) |alias| if (std.mem.eql(u8, alias, name)) return true;
    }
    if (T == ast.Expr) if (value.value == .variable) return std.mem.eql(u8, value.value.variable, name);
    if (T == ast.MapSelector) if (value == .variable) return std.mem.eql(u8, value.variable, name);
    switch (@typeInfo(T)) {
        .pointer => |p| {
            if (p.size == .one and p.child == ast.Expr) return references(value.*, name);
            if (p.size == .slice and p.child != u8) for (value) |child| {
                if (references(child, name)) return true;
            };
        },
        .@"struct" => |info| inline for (info.field_names) |field_name| {
            if (references(@field(value, field_name), name)) return true;
        },
        .@"union" => switch (value) {
            inline else => |child| return references(child, name),
        },
        .optional => if (value) |child| return references(child, name),
        else => {},
    }
    return false;
}
pub fn prune(a: A, original: ast.Path, consumers: anytype) !ast.Path {
    var path = original;
    inline for (@typeInfo(ast.PathSummary).@"struct".field_names) |field| {
        if (@field(path.execution_summary, field)) |name| {
            if (!references(consumers, name)) @field(path.execution_summary, field) = null;
        }
    }
    var changed = false;
    for (path.edges) |edge| if (edge.execution_length) |name| {
        if (!references(consumers, name)) {
            changed = true;
            break;
        }
    };
    if (changed) {
        const edges = try a.dupe(ast.Edge, path.edges);
        for (edges) |*edge| if (edge.execution_length) |name| {
            if (!references(consumers, name)) edge.execution_length = null;
        };
        path.edges = edges;
    }
    return path;
}
