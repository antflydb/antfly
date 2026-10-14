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

//! Validate every branch before opening storage or staging a mutation. Empty
//! input never suppresses an unknown label, procedure, function, or variable.
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
pub const Kind = enum { node, relationship, path, list, scalar, unknown };
const Symbol = struct { name: []const u8, kind: Kind };
const Scope = std.ArrayList(Symbol);
pub fn validate(a: std.mem.Allocator, graph: catalog.Graph, query: ast.Query) !void {
    var memory: @import("budget.zig") = .{ .backing = a, .limit = 64 << 20 };
    var scratch = std.heap.ArenaAllocator.init(memory.allocator());
    defer scratch.deinit();
    var binder: Binder = .{ .a = scratch.allocator(), .graph = graph, .source = query.source };
    const first = try binder.clauses(query.clauses, .empty, false);
    for (query.unions) |arm| {
        const other = try binder.clauses(arm.clauses, .empty, false);
        if (first.items.len != other.items.len) return error.CypherUnionColumnMismatch;
        for (first.items, other.items) |left, right| if (!std.mem.eql(u8, left.name, right.name)) return error.CypherUnionColumnMismatch;
    }
}
/// Own the projected column names without executing or opening storage.
pub fn columns(a: std.mem.Allocator, graph: catalog.Graph, query: ast.Query) ![]const []const u8 {
    var memory: @import("budget.zig") = .{ .backing = a, .limit = 64 << 20 };
    var scratch = std.heap.ArenaAllocator.init(memory.allocator());
    defer scratch.deinit();
    var binder: Binder = .{ .a = scratch.allocator(), .graph = graph, .source = query.source };
    const output = try binder.clauses(query.clauses, .empty, false);
    const names = try a.alloc([]const u8, output.items.len);
    var initialized: usize = 0;
    errdefer {
        for (names[0..initialized]) |name| a.free(name);
        a.free(names);
    }
    for (output.items, names) |symbol, *name| {
        name.* = try a.dupe(u8, symbol.name);
        initialized += 1;
    }
    return names;
}
const Binder = struct {
    a: std.mem.Allocator,
    graph: catalog.Graph,
    source: []const u8,
    fn find(scope: Scope, name: []const u8) ?Kind {
        for (scope.items) |symbol| if (std.mem.eql(u8, symbol.name, name)) return symbol.kind;
        return null;
    }
    fn declare(self: *Binder, scope: *Scope, name: ?[]const u8, kind: Kind) !void {
        const alias = name orelse return;
        if (find(scope.*, alias)) |existing| {
            if (existing != kind and existing != .unknown and kind != .unknown) return error.CypherVariableTypeConflict;
            return;
        }
        try scope.append(self.a, .{ .name = alias, .kind = kind });
    }
    fn patternDeclarations(self: *Binder, scope: *Scope, path: ast.Path, create: bool) anyerror!void {
        if (path.selector != .all and (path.edges.len != 1 or create)) return error.CypherInvalidShortestPath;
        for (path.nodes) |node| {
            for (node.labels) |label| if (self.graph.label(label) == null) return error.UnknownGraphLabel;
            // Bound nodes may be reused as endpoints, but CREATE/MERGE
            // cannot redeclare their labels or properties.
            if (create) if (node.alias) |alias| {
                if (find(scope.*, alias) != null and (node.labels.len != 0 or node.properties.len != 0)) return error.CypherVariableConflict;
            };
            try self.declare(scope, node.alias, .node);
        }
        for (path.edges) |edge| {
            for (edge.types) |name| if (self.graph.relationship(name) == null) return error.UnknownGraphRelationship;
            if (create and (edge.variable_length or edge.types.len != 1 or edge.direction == .both)) return error.CypherInvalidCreateRelationship;
            if (create) if (edge.alias) |alias| {
                if (find(scope.*, alias) != null) return error.CypherVariableConflict;
            };
            try self.declare(scope, edge.alias, if (edge.variable_length) .list else .relationship);
        }
        if (create) if (path.alias) |alias| {
            if (find(scope.*, alias) != null) return error.CypherVariableConflict;
        };
        try self.declare(scope, path.alias, .path);
    }
    fn pattern(self: *Binder, scope: *Scope, path: ast.Path, create: bool) anyerror!void {
        try self.patternDeclarations(scope, path, create);
        try self.patternProperties(scope.*, path);
    }
    fn patternProperties(self: *Binder, scope: Scope, path: ast.Path) anyerror!void {
        for (path.nodes) |node| for (node.properties) |property| try self.expression(scope, property.value, false);
        for (path.edges) |edge| for (edge.properties) |property| try self.expression(scope, property.value, false);
    }
    fn clauses(self: *Binder, input: []const ast.Clause, initial: Scope, nested: bool) anyerror!Scope {
        var scope: Scope = .empty;
        try scope.appendSlice(self.a, initial.items);
        var output: Scope = .empty;
        var returned = false;
        var writes = false;
        for (input) |clause| {
            if (returned) return error.CypherReturnMustBeLast;
            switch (clause.operation) {
                .match => |m| {
                    for (m.paths) |path| try self.patternDeclarations(&scope, path, false);
                    for (m.paths) |path| try self.patternProperties(scope, path);
                    if (m.predicate) |predicate| try self.expression(scope, predicate, false);
                },
                .project => |p| {
                    output = .empty;
                    const expression_rules = @import("expressions.zig");
                    var grouped = false;
                    for (p.value.items) |item| grouped = grouped or expression_rules.aggregates(item.expression);
                    if (grouped) {
                        if (p.value.wildcard) return error.CypherInvalidGrouping;
                        for (p.value.items) |item| if (expression_rules.aggregates(item.expression)) try expression_rules.validateGrouping(item.expression, p.value.items, false);
                        for (p.value.order) |order| try expression_rules.validateGrouping(order.expression, p.value.items, true);
                    }
                    if (p.value.wildcard) try output.appendSlice(self.a, scope.items);
                    for (p.value.items) |item| {
                        try self.expression(scope, item.expression, true);
                        const name = item.alias orelse try self.expressionName(item.expression);
                        if (find(output, name) != null) return error.DuplicateCypherColumn;
                        try self.declare(&output, name, switch (item.expression.value) {
                            .variable => |variable| find(scope, variable).?,
                            else => .unknown,
                        });
                    }
                    var order_scope = scope;
                    if (p.value.distinct) order_scope = output else {
                        order_scope = .empty;
                        try order_scope.appendSlice(self.a, scope.items);
                        for (output.items) |symbol| {
                            var replaced = false;
                            for (order_scope.items) |*existing| if (std.mem.eql(u8, existing.name, symbol.name)) {
                                existing.* = symbol;
                                replaced = true;
                                break;
                            };
                            if (!replaced) try order_scope.append(self.a, symbol);
                        }
                    }
                    for (p.value.order) |order| try self.expression(order_scope, order.expression, true);
                    if (p.value.skip) |skip| try self.expression(.empty, skip, false);
                    if (p.value.limit) |limit| try self.expression(.empty, limit, false);
                    if (p.value.predicate) |predicate| {
                        if (p.final) return error.CypherReturnPredicate;
                        try self.expression(if (grouped or p.value.distinct) output else order_scope, predicate, false);
                    }
                    scope = output;
                    returned = p.final;
                },
                .unwind => |u| {
                    try self.expression(scope, u.expression, false);
                    if (find(scope, u.alias) != null) return error.CypherVariableConflict;
                    try self.declare(&scope, u.alias, .unknown);
                },
                .call => |c| {
                    if (!std.mem.eql(u8, c.name, "antfly.search")) return error.UnknownCypherProcedure;
                    if (c.args.len != 2) return error.CypherFunctionArity;
                    for (c.args) |arg| try self.expression(scope, arg, false);
                    for (c.yields) |yield| {
                        if (find(scope, yield.alias) != null) return error.CypherVariableConflict;
                        const kind: Kind = if (std.mem.eql(u8, yield.name, "node")) .node else if (std.mem.eql(u8, yield.name, "score")) .scalar else return error.UnknownCypherYield;
                        try self.declare(&scope, yield.alias, kind);
                    }
                    if (c.predicate) |predicate| try self.expression(scope, predicate, false);
                },
                .subquery => |inner| {
                    const exports = try self.clauses(inner, scope, true);
                    for (exports.items) |symbol| {
                        if (find(scope, symbol.name) != null) return error.CypherVariableConflict;
                        try scope.append(self.a, symbol);
                    }
                },
                .create => |paths| {
                    writes = true;
                    for (paths) |path| try self.pattern(&scope, path, true);
                },
                .merge => |m| {
                    writes = true;
                    try self.pattern(&scope, m.path, true);
                    try self.assignments(scope, m.on_create);
                    try self.assignments(scope, m.on_match);
                },
                .set => |values| {
                    writes = true;
                    try self.assignments(scope, values);
                },
                .remove => |expressions| {
                    writes = true;
                    for (expressions) |expression_value| {
                        if (expression_value.value != .property) return error.CypherRemoveRequiresProperty;
                        try self.expression(scope, expression_value, false);
                    }
                },
                .delete => |d| {
                    writes = true;
                    for (d.expressions) |expression_value| try self.expression(scope, expression_value, false);
                },
            }
        }
        if (!returned and !writes and !nested) return error.CypherQueryRequiresReturn;
        return if (returned) output else .empty;
    }
    fn assignments(self: *Binder, scope: Scope, values: []const ast.Assignment) !void {
        for (values) |assignment| {
            if (assignment.target.value != .variable and assignment.target.value != .property) return error.CypherInvalidAssignment;
            try self.expression(scope, assignment.target, false);
            try self.expression(scope, assignment.value, false);
        }
    }
    fn localScope(self: *Binder, scope: Scope, names: []const []const u8) !Scope {
        var inner: Scope = .empty;
        try inner.appendSlice(self.a, scope.items);
        for (names) |name| {
            var shadows = false;
            for (inner.items) |*symbol| if (std.mem.eql(u8, symbol.name, name)) {
                symbol.kind = .unknown;
                shadows = true;
                break;
            };
            if (!shadows) try self.declare(&inner, name, .unknown);
        }
        return inner;
    }
    fn expression(self: *Binder, scope: Scope, expr: *const ast.Expr, aggregates: bool) anyerror!void {
        switch (expr.value) {
            .pattern_comprehension => |c| {
                var inner: Scope = .empty;
                for (scope.items) |symbol| {
                    if (c.path.alias) |alias| if (std.mem.eql(u8, symbol.name, alias)) continue;
                    try inner.append(self.a, symbol);
                }
                try self.pattern(&inner, c.path, false);
                if (c.predicate) |predicate| try self.expression(inner, predicate, false);
                try self.expression(inner, c.projection, false);
            },
            .list_predicate => |c| {
                try self.expression(scope, c.input, aggregates);
                const inner = try self.localScope(scope, &.{c.alias});
                try self.expression(inner, c.predicate, false);
            },
            .reduce => |c| {
                try self.expression(scope, c.initial, aggregates);
                try self.expression(scope, c.input, aggregates);
                const inner = try self.localScope(scope, &.{ c.accumulator, c.alias });
                try self.expression(inner, c.projection, false);
            },
            .comprehension => |c| {
                try self.expression(scope, c.input, aggregates);
                var inner: Scope = .empty;
                try inner.appendSlice(self.a, scope.items);
                var shadows = false;
                for (inner.items) |*symbol| if (std.mem.eql(u8, symbol.name, c.alias)) {
                    symbol.kind = .unknown;
                    shadows = true;
                    break;
                };
                if (!shadows) try self.declare(&inner, c.alias, .unknown);
                if (c.predicate) |predicate| try self.expression(inner, predicate, false);
                if (c.projection) |projection| try self.expression(inner, projection, false);
            },
            .variable => |name| if (find(scope, name) == null) return error.UnboundCypherVariable,
            .literal, .parameter => {},
            .property => |p| try self.expression(scope, p.base, aggregates),
            .index => |i| {
                try self.expression(scope, i.base, aggregates);
                try self.expression(scope, i.index, aggregates);
            },
            .slice => |s| {
                try self.expression(scope, s.base, aggregates);
                if (s.start) |bound| try self.expression(scope, bound, aggregates);
                if (s.end) |bound| try self.expression(scope, bound, aggregates);
            },
            .unary => |u| try self.expression(scope, u.operand, aggregates),
            .binary => |b| {
                try self.expression(scope, b.left, aggregates);
                try self.expression(scope, b.right, aggregates);
                if (b.op == .regex) {
                    for ([_]*const ast.Expr{ b.left, b.right }) |operand| if (operand.value == .literal and operand.value.literal != .null and operand.value.literal != .string) return error.CypherTypeMismatch;
                    if (b.right.value == .literal and b.right.value.literal == .string) try @import("regex.zig").validate(self.a, b.right.value.literal.string);
                }
            },
            .comparison => |c| for (c.operands) |operand| try self.expression(scope, operand, aggregates),
            .list => |items| for (items) |item| try self.expression(scope, item, aggregates),
            .map => |items| for (items) |item| try self.expression(scope, item.value, aggregates),
            .map_projection => |p| {
                try self.expression(scope, p.base, aggregates);
                for (p.items) |item| switch (item) {
                    .variable => |name| if (find(scope, name) == null) return error.UnboundCypherVariable,
                    .entry => |entry| try self.expression(scope, entry.value, aggregates),
                    .property, .all => {},
                };
            },
            .case_when => |c| {
                if (c.operand) |operand| try self.expression(scope, operand, aggregates);
                for (c.branches) |branch| {
                    try self.expression(scope, branch.condition, aggregates);
                    try self.expression(scope, branch.value, aggregates);
                }
                if (c.otherwise) |other| try self.expression(scope, other, aggregates);
            },
            .exists => |inner| {
                try readOnly(inner);
                _ = try self.clauses(inner, scope, true);
            },
            .call => |c| {
                var aggregate = false;
                for ([_][]const u8{ "count", "sum", "avg", "min", "max", "collect" }) |name| if (std.ascii.eqlIgnoreCase(c.name, name)) {
                    aggregate = true;
                    break;
                };
                if (aggregate and !aggregates) return error.CypherAggregateScope;
                if (aggregate) {
                    if (c.star and (!std.ascii.eqlIgnoreCase(c.name, "count") or c.distinct)) return error.CypherFunctionArity;
                    if (c.args.len != (if (c.star) @as(usize, 0) else 1)) return error.CypherFunctionArity;
                } else {
                    if (std.ascii.eqlIgnoreCase(c.name, "id")) return error.CypherLegacyIdUnavailable;
                    const signature = @import("functions.zig").lookup(c.name) orelse return error.UnknownCypherFunction;
                    if (c.star or c.distinct or c.args.len < signature.minimum or c.args.len > signature.maximum) return error.CypherFunctionArity;
                }
                for (c.args) |arg| try self.expression(scope, arg, if (aggregate) false else aggregates);
            },
        }
    }
    fn expressionName(self: *Binder, expr: *const ast.Expr) anyerror![]const u8 {
        if (expr.value == .map_projection and expr.value.map_projection.base.value == .variable) return expr.value.map_projection.base.value.variable;
        if (expr.value != .variable and expr.value != .property and expr.span.end <= self.source.len) return self.a.dupe(u8, self.source[expr.span.start..expr.span.end]);
        return switch (expr.value) {
            .variable => |name| name,
            .property => |p| std.fmt.allocPrint(self.a, "{s}.{s}", .{ try self.expressionName(p.base), p.name }),
            .call => |c| std.fmt.allocPrint(self.a, "{s}({s})", .{ c.name, if (c.star) "*" else if (c.args.len == 1) try self.expressionName(c.args[0]) else "..." }),
            else => std.fmt.allocPrint(self.a, "expression@{}", .{expr.span.start}),
        };
    }
};

fn readOnly(clauses: []const ast.Clause) error{CypherExistsMutation}!void {
    for (clauses) |clause| switch (clause.operation) {
        .create, .merge, .set, .remove, .delete => return error.CypherExistsMutation,
        .subquery => |inner| try readOnly(inner),
        else => {},
    };
}
