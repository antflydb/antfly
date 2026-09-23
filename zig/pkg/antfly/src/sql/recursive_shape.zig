// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Admission for linear recursion. Analysis is syntax-only and never reads data.
const std = @import("std");
const ast = @import("ast.zig");

fn scalarReferences(value: *const ast.Scalar, name: []const u8, depth: usize) anyerror!usize {
    if (depth > 64) return error.SqlProgramLimitExceeded;
    return switch (value.*) {
        .call => |call| blk: {
            var count: usize = if (call.subquery) |query| try references(query.*, name, depth + 1) else 0;
            for (call.args) |arg| count += try scalarReferences(arg, name, depth + 1);
            if (call.filter) |filter| count += try scalarReferences(filter, name, depth + 1);
            break :blk count;
        },
        .binary => |part| try scalarReferences(part.left, name, depth + 1) + try scalarReferences(part.right, name, depth + 1),
        .unary => |part| scalarReferences(part.operand, name, depth + 1),
        .cast => |part| scalarReferences(part.operand, name, depth + 1),
        .case_when => |part| blk: {
            var count: usize = if (part.otherwise) |other| try scalarReferences(other, name, depth + 1) else 0;
            for (part.branches) |branch| count += try scalarReferences(branch.condition, name, depth + 1) + try scalarReferences(branch.value, name, depth + 1);
            break :blk count;
        },
        .in_list => |part| blk: {
            var count = try scalarReferences(part.operand, name, depth + 1);
            for (part.values) |item| count += try scalarReferences(item, name, depth + 1);
            break :blk count;
        },
        else => 0,
    };
}
fn predicateReferences(value: *const ast.Predicate, name: []const u8, depth: usize) anyerror!usize {
    if (depth > 64) return error.SqlProgramLimitExceeded;
    return switch (value.*) {
        .scalar => |part| scalarReferences(part, name, depth + 1),
        .negation => |part| predicateReferences(part, name, depth + 1),
        .conjunction, .disjunction => |part| try predicateReferences(part.left, name, depth + 1) + try predicateReferences(part.right, name, depth + 1),
        else => 0,
    };
}
fn relationReferences(value: *const ast.Relation, name: []const u8, depth: usize) anyerror!usize {
    if (depth > 64) return error.SqlProgramLimitExceeded;
    return switch (value.*) {
        .table => |table| @intFromBool(table.name.database == null and table.name.namespace == null and std.mem.eql(u8, table.name.table, name)),
        .derived => |derived| references(derived.query.*, name, depth + 1),
        .join => |join| try relationReferences(join.left, name, depth + 1) + try relationReferences(join.right, name, depth + 1) + if (join.condition) |condition| try scalarReferences(condition, name, depth + 1) else @as(usize, 0),
    };
}
pub fn references(query: ast.Select, name: []const u8, depth: usize) anyerror!usize {
    if (depth > 64) return error.SqlProgramLimitExceeded;
    // An inner WITH declaration shadows this relation name in its own query.
    for (query.ctes) |cte| if (std.mem.eql(u8, cte.name, name)) return 0;
    var count: usize = 0;
    for (query.ctes) |cte| count += try references(cte.query.*, name, depth + 1);
    for (query.values_arms) |arm| count += try references(arm.*, name, depth + 1);
    if (query.set_operation) |set| return count + try references(set.left.*, name, depth + 1) + try references(set.right.*, name, depth + 1);
    if (query.source) |source| count += try relationReferences(source, name, depth + 1) else if (query.table) |table| {
        count += @intFromBool(table.database == null and table.namespace == null and std.mem.eql(u8, table.table, name));
    }
    for (query.columns) |projection| if (projection.expression) |expression| {
        count += try scalarReferences(expression, name, depth + 1);
    };
    if (query.predicate) |predicate| count += try predicateReferences(predicate, name, depth + 1);
    if (query.having) |having| count += try scalarReferences(having, name, depth + 1);
    for (query.group_by) |group| count += try scalarReferences(group, name, depth + 1);
    for (query.order_by) |order| if (order.expression) |expression| {
        count += try scalarReferences(expression, name, depth + 1);
    };
    return count;
}

fn validateRelation(value: *const ast.Relation, name: []const u8, depth: usize) anyerror!void {
    if (depth > 64) return error.SqlProgramLimitExceeded;
    switch (value.*) {
        .table => {},
        .derived => |derived| if (try references(derived.query.*, name, depth + 1) != 0) return error.UnsupportedSqlShape,
        .join => |join| {
            const left = try relationReferences(join.left, name, depth + 1);
            const right = try relationReferences(join.right, name, depth + 1);
            if ((left != 0 and (join.kind == .right or join.kind == .full)) or (right != 0 and (join.kind == .left or join.kind == .full))) return error.UnsupportedSqlShape;
            if (join.condition) |condition| if (try scalarReferences(condition, name, depth + 1) != 0) return error.UnsupportedSqlShape;
            try validateRelation(join.left, name, depth + 1);
            try validateRelation(join.right, name, depth + 1);
        },
    }
}
pub fn validate(cte: ast.Cte) !void {
    const set = cte.query.set_operation orelse return error.UnsupportedSqlShape;
    if (set.kind != .@"union" or cte.query.order_by.len != 0 or cte.query.limit != null or cte.query.offset != null) return error.UnsupportedSqlShape;
    if (try references(set.left.*, cte.name, 0) != 0 or try references(set.right.*, cte.name, 0) != 1) return error.UnsupportedSqlShape;
    const step = set.right.*;
    if (step.set_operation != null or step.count_all or @import("aggregate_binding.zig").accepts(step) or @import("window_binding.zig").accepts(step)) return error.UnsupportedSqlShape;
    for (step.ctes) |nested| if (try references(nested.query.*, cte.name, 0) != 0) return error.UnsupportedSqlShape;
    for (step.columns) |projection| if (projection.expression) |expression| if (try scalarReferences(expression, cte.name, 0) != 0) return error.UnsupportedSqlShape;
    if (step.predicate) |predicate| if (try predicateReferences(predicate, cte.name, 0) != 0) return error.UnsupportedSqlShape;
    if (step.source) |source| try validateRelation(source, cte.name, 0);
}
