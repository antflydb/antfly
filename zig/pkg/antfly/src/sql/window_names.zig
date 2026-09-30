// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Compile-time window scope resolution. Runs before the owned AST becomes
//! immutable. No catalog/data reads; named specifications share expression
//! nodes and subsequently share bound sort domains rather than cloning trees.
const std = @import("std");
const ast = @import("ast.zig");
const Error = std.mem.Allocator.Error || error{ InvalidSqlSyntax, SqlLimitExceeded };
const Map = std.StringHashMapUnmanaged(ast.Window);

fn inherit(input: ast.Window, names: *const Map) Error!ast.Window {
    const name = input.reference orelse return input;
    const base = names.get(name) orelse return error.InvalidSqlSyntax;
    if (!input.copy_reference) return base;
    // Copying a named window differs from OVER name: framed windows cannot
    // be copied, PARTITION cannot be added, and ORDER cannot be replaced.
    if (base.frame != null or input.partition.len != 0 or (base.order.len != 0 and input.order.len != 0)) return error.InvalidSqlSyntax;
    var result = input;
    result.reference = null;
    result.copy_reference = false;
    result.partition = base.partition;
    if (result.order.len == 0) result.order = base.order;
    return result;
}

const Resolver = struct {
    names: *const Map,
    max_depth: usize,
    fn scalar(self: @This(), node: *const ast.Scalar, depth: usize) Error!void {
        if (depth >= self.max_depth) return error.SqlLimitExceeded;
        // Every node was allocated by this compiler invocation. Mutation ends
        // before Compiled is published; execution never mutates these nodes.
        switch (@constCast(node).*) {
            .call => |*call| {
                for (call.args) |arg| try self.scalar(arg, depth + 1);
                if (call.filter) |filter| try self.scalar(filter, depth + 1);
                if (call.window) |window| {
                    call.window = try inherit(window, self.names);
                    for (call.window.?.partition) |part| try self.scalar(part, depth + 1);
                    for (call.window.?.order) |order| if (order.expression) |part| try self.scalar(part, depth + 1);
                }
                // Subqueries were resolved in their own selectCore scope.
            },
            .unary => |part| try self.scalar(part.operand, depth + 1),
            .binary => |part| {
                try self.scalar(part.left, depth + 1);
                try self.scalar(part.right, depth + 1);
            },
            .cast => |part| try self.scalar(part.operand, depth + 1),
            .case_when => |part| {
                for (part.branches) |branch| {
                    try self.scalar(branch.condition, depth + 1);
                    try self.scalar(branch.value, depth + 1);
                }
                if (part.otherwise) |otherwise| try self.scalar(otherwise, depth + 1);
            },
            .in_list => |part| {
                try self.scalar(part.operand, depth + 1);
                for (part.values) |item| try self.scalar(item, depth + 1);
            },
            .literal, .column => {},
        }
    }
    fn predicate(self: @This(), node: *const ast.Predicate, depth: usize) Error!void {
        if (depth >= self.max_depth) return error.SqlLimitExceeded;
        switch (node.*) {
            .scalar => |value| try self.scalar(value, depth + 1),
            .conjunction, .disjunction => |part| {
                try self.predicate(part.left, depth + 1);
                try self.predicate(part.right, depth + 1);
            },
            .negation => |part| try self.predicate(part, depth + 1),
            .comparison, .is_null => {},
        }
    }
};

pub fn resolveCore(alloc: std.mem.Allocator, query: *ast.Select, max_depth: usize) Error!void {
    var names: Map = .empty;
    defer names.deinit(alloc);
    for (@constCast(query.windows)) |*definition| {
        // Reject nested window functions even in unused definitions. This
        // also rules out recursive expression graphs before name expansion.
        for (definition.window.partition) |part| if (@import("window_binding.zig").contains(part)) return error.InvalidSqlSyntax;
        for (definition.window.order) |order| if (order.expression) |part| if (@import("window_binding.zig").contains(part)) return error.InvalidSqlSyntax;
        definition.window = try inherit(definition.window, &names);
        const entry = try names.getOrPut(alloc, definition.name);
        if (entry.found_existing) return error.InvalidSqlSyntax;
        entry.value_ptr.* = definition.window;
    }
    const resolver = Resolver{ .names = &names, .max_depth = max_depth };
    for (query.columns) |column| if (column.expression) |expression| try resolver.scalar(expression, 0);
    if (query.predicate) |predicate| try resolver.predicate(predicate, 0);
    for (query.group_by) |group| try resolver.scalar(group, 0);
    if (query.having) |having| try resolver.scalar(having, 0);
}

pub fn resolveOrder(alloc: std.mem.Allocator, query: ast.Select, max_depth: usize) Error!void {
    if (query.order_by.len == 0) return;
    var names: Map = .empty;
    defer names.deinit(alloc);
    for (query.windows) |definition| try names.put(alloc, definition.name, definition.window);
    const resolver = Resolver{ .names = &names, .max_depth = max_depth };
    for (query.order_by) |order| if (order.expression) |expression| try resolver.scalar(expression, 0);
}
