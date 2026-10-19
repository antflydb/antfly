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

//! Invocation-local range extraction. Only total integer comparisons on one
//! ALL-path segment are movable; eager Boolean siblings and selectors remain
//! barriers unless the complete predicate has a proof.
const std = @import("std");
const ast = @import("ast.zig");
const v = @import("value.zig");
const Form = @import("integer_affine.zig").Form;
const A = std.mem.Allocator;
const Range = struct { low: i128, high: i128 };
fn integer(expression: *const ast.Expr, parameters: []const v.Entry) ?i64 {
    return switch (expression.value) {
        .literal => |value| if (value == .integer) value.integer else null,
        .parameter => |name| blk: {
            for (parameters) |entry| if (std.mem.eql(u8, entry.name, name)) break :blk if (entry.value == .integer) entry.value.integer else null;
            break :blk null;
        },
        .unary => |u| blk: {
            const value = integer(u.operand, parameters) orelse return null;
            break :blk switch (u.op) {
                .positive => value,
                .negative => std.math.negate(value) catch null,
                else => null,
            };
        },
        .binary => |b| blk: {
            const left = Form.literal(integer(b.left, parameters) orelse return null);
            const right = Form.literal(integer(b.right, parameters) orelse return null);
            const result = switch (b.op) {
                .add => Form.combine(left, right, .add),
                .subtract => Form.combine(left, right, .subtract),
                .multiply => Form.combine(left, right, .multiply),
                else => null,
            } orelse return null;
            break :blk result.offset;
        },
        else => null,
    };
}
fn totalProperty(expression: *const ast.Expr, parameters: []const v.Entry) bool {
    if (expression.value == .literal) return true;
    if (expression.value == .parameter) {
        for (parameters) |entry| if (std.mem.eql(u8, entry.name, expression.value.parameter)) return switch (entry.value) {
            .null, .boolean, .integer, .number, .string => true,
            else => false,
        };
        return false;
    }
    return integer(expression, parameters) != null;
}
fn totalProperties(path: ast.Path, parameters: []const v.Entry) bool {
    for (path.nodes) |node| for (node.properties) |entry| if (!totalProperty(entry.value, parameters)) return false;
    for (path.edges) |edge| for (edge.properties) |entry| if (!totalProperty(entry.value, parameters)) return false;
    return true;
}
fn length(expression: *const ast.Expr, path: ast.Path) bool {
    if (expression.value != .variable) return false;
    const name = expression.value.variable;
    if (path.execution_summary.length) |slot| if (std.mem.eql(u8, slot, name)) return true;
    if (path.edges[0].execution_length) |slot| if (std.mem.eql(u8, slot, name)) return true;
    return false;
}
fn comparison(left: *const ast.Expr, op: ast.Binary, right: *const ast.Expr, path: ast.Path, parameters: []const v.Entry, range: *Range) bool {
    var operation = op;
    const bound: i128 = if (length(left, path)) integer(right, parameters) orelse return false else if (length(right, path)) blk: {
        operation = switch (op) {
            .lt => .gt,
            .lte => .gte,
            .gt => .lt,
            .gte => .lte,
            else => op,
        };
        break :blk integer(left, parameters) orelse return false;
    } else return false;
    switch (operation) {
        .eq => {
            range.low = @max(range.low, bound);
            range.high = @min(range.high, bound);
        },
        .gt => range.low = @max(range.low, bound + 1),
        .gte => range.low = @max(range.low, bound),
        .lt => range.high = @min(range.high, bound - 1),
        .lte => range.high = @min(range.high, bound),
        else => return false,
    }
    return true;
}
fn predicate(expression: *const ast.Expr, path: ast.Path, parameters: []const v.Entry, range: *Range) bool {
    return switch (expression.value) {
        .binary => |b| if (b.op == .@"and") predicate(b.left, path, parameters, range) and predicate(b.right, path, parameters, range) else comparison(b.left, b.op, b.right, path, parameters, range),
        .comparison => |c| blk: {
            for (c.operators, 0..) |op, i| if (!comparison(c.operands[i], op, c.operands[i + 1], path, parameters, range)) break :blk false;
            break :blk true;
        },
        else => false,
    };
}
fn clauses(a: A, input: []const ast.Clause, parameters: []const v.Entry) anyerror![]const ast.Clause {
    const result = try a.dupe(ast.Clause, input);
    for (result) |*clause| switch (clause.operation) {
        .subquery => |inner| clause.operation.subquery = try clauses(a, inner, parameters),
        .match => |*operation| {
            const guard = operation.predicate orelse continue;
            if (operation.paths.len != 1) continue;
            const path = operation.paths[0];
            if (path.selector != .all or path.edges.len != 1 or path.execution_prefix) continue;
            // Property constraints run before WHERE, including at endpoints
            // that the guard rejects. Never prune those evaluation boundaries.
            if (!totalProperties(path, parameters)) continue;
            var range: Range = .{ .low = path.edges[0].min_hops, .high = path.edges[0].max_hops orelse std.math.maxInt(i64) };
            if (!predicate(guard, path, parameters, &range)) continue;
            if (range.low > range.high) {
                const empty = try a.create(ast.Expr);
                empty.* = .{ .span = guard.span, .value = .{ .literal = .{ .boolean = false } } };
                operation.predicate = empty;
                continue;
            }
            if (range.low > std.math.maxInt(u32) or (range.high > std.math.maxInt(u32) and range.high != std.math.maxInt(i64))) continue;
            const paths = try a.dupe(ast.Path, operation.paths);
            const edges = try a.dupe(ast.Edge, path.edges);
            edges[0].min_hops = @intCast(range.low);
            edges[0].max_hops = if (range.high == std.math.maxInt(i64)) null else @intCast(range.high);
            paths[0].edges = edges;
            operation.paths = paths;
            operation.predicate = null;
        },
        else => {},
    };
    return result;
}
pub fn normalize(a: A, query: ast.Query, parameters: []const v.Entry) !ast.Query {
    var result = query;
    result.clauses = try clauses(a, query.clauses, parameters);
    const unions = try a.dupe(ast.UnionArm, query.unions);
    for (unions) |*arm| arm.clauses = try clauses(a, arm.clauses, parameters);
    result.unions = unions;
    return result;
}
