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
const Origin = enum { length, node, value };
const Names = std.StringHashMapUnmanaged(Origin);
fn length(expression: *const ast.Expr, names: Names) bool {
    return expression.value == .variable and names.get(expression.value.variable) == .length;
}
fn scalarRead(expression: *const ast.Expr, names: Names, parameters: []const v.Entry) bool {
    return switch (expression.value) {
        .literal => true,
        .parameter => totalProperty(expression, parameters),
        .property => |property| property.base.value == .variable and names.get(property.base.value.variable) == .node,
        else => false,
    };
}
fn comparison(left: *const ast.Expr, op: ast.Binary, right: *const ast.Expr, names: Names, parameters: []const v.Entry, range: *Range) bool {
    var operation = op;
    const bound: i128 = if (length(left, names)) integer(right, parameters) orelse return false else if (length(right, names)) blk: {
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
const Extracted = struct { residual: ?*const ast.Expr = null, observed: bool = false };
fn extract(a: A, expression: *const ast.Expr, names: Names, parameters: []const v.Entry, range: *Range) !?Extracted {
    switch (expression.value) {
        .binary => |binary| {
            if (binary.op == .@"and") {
                const left = (try extract(a, binary.left, names, parameters, range)) orelse return null;
                const right = (try extract(a, binary.right, names, parameters, range)) orelse return null;
                var residual = left.residual orelse right.residual;
                if (left.residual != null and right.residual != null) {
                    const joined = try a.create(ast.Expr);
                    joined.* = .{ .span = expression.span, .value = .{ .binary = .{ .op = .@"and", .left = left.residual.?, .right = right.residual.? } } };
                    residual = joined;
                }
                return .{ .residual = residual, .observed = left.observed or right.observed };
            }
            if (comparison(binary.left, binary.op, binary.right, names, parameters, range)) return .{ .observed = true };
            if ((binary.op == .eq or binary.op == .neq) and scalarRead(binary.left, names, parameters) and scalarRead(binary.right, names, parameters)) return .{ .residual = expression };
        },
        .comparison => |chain| {
            for (chain.operators, 0..) |op, i| if (!comparison(chain.operands[i], op, chain.operands[i + 1], names, parameters, range)) return null;
            return .{ .observed = true };
        },
        .literal => |value| {
            if (value == .boolean) return .{ .residual = if (value.boolean) null else expression };
            if (value == .null) return .{ .residual = expression };
        },
        else => {},
    }
    return null;
}
fn project(a: A, projection: ast.Projection, names: Names, parameters: []const v.Entry) !?Names {
    if (projection.distinct or projection.skip != null or projection.limit != null or projection.order.len != 0) return null;
    var next: Names = if (projection.wildcard) try names.clone(a) else .empty;
    for (projection.items) |item| {
        const expression = item.expression;
        const origin: Origin = if (expression.value == .variable) names.get(expression.value.variable) orelse return null else if (scalarRead(expression, names, parameters)) .value else return null;
        const name = item.alias orelse if (expression.value == .variable) expression.value.variable else return null;
        try next.put(a, name, origin);
    }
    return next;
}
fn clauses(a: A, input: []const ast.Clause, parameters: []const v.Entry) anyerror![]const ast.Clause {
    const result = try a.dupe(ast.Clause, input);
    for (result, 0..) |*clause, index| switch (clause.operation) {
        .subquery => |inner| clause.operation.subquery = try clauses(a, inner, parameters),
        .match => |*operation| {
            if (operation.paths.len != 1) continue;
            const path = operation.paths[0];
            if (path.selector != .all or path.edges.len != 1 or path.execution_prefix) continue;
            if (path.execution_summary.length == null and path.edges[0].execution_length == null) continue;
            // Property constraints precede WHERE, including at rejected endpoints.
            // Every boundary crossed by a range must be total, not merely pure.
            if (!totalProperties(path, parameters)) continue;
            var names: Names = .empty;
            for (path.nodes) |node| if (node.alias) |name| try names.put(a, name, .node);
            if (path.execution_summary.length) |name| try names.put(a, name, .length);
            if (path.edges[0].execution_length) |name| try names.put(a, name, .length);
            var range: Range = .{ .low = path.edges[0].min_hops, .high = path.edges[0].max_hops orelse std.math.maxInt(i64) };
            const Pending = struct { index: usize, residual: ?*const ast.Expr };
            var pending: std.ArrayList(Pending) = .empty;
            var observed = false;
            if (operation.predicate) |guard| {
                const extracted = (try extract(a, guard, names, parameters, &range)) orelse continue;
                observed = extracted.observed;
                try pending.append(a, .{ .index = index, .residual = extracted.residual });
            }
            for (result[index + 1 ..], index + 1..) |following, position| {
                if (operation.optional) break;
                if (following.operation != .project or following.operation.project.final) break;
                const projection = following.operation.project.value;
                names = (try project(a, projection, names, parameters)) orelse break;
                if (projection.predicate) |guard| {
                    // An unsafe later filter is a barrier, not a reason to
                    // discard proofs already established before it.
                    var candidate = range;
                    const extracted = (try extract(a, guard, names, parameters, &candidate)) orelse break;
                    range = candidate;
                    observed = observed or extracted.observed;
                    try pending.append(a, .{ .index = position, .residual = extracted.residual });
                }
            }
            if (!observed) continue;
            if (range.low > range.high) {
                const empty = try a.create(ast.Expr);
                empty.* = .{ .span = clause.span, .value = .{ .literal = .{ .boolean = false } } };
                operation.predicate = empty;
                // Keep subsequent predicates intact; no rows reach them.
                continue;
            }
            if (range.low > std.math.maxInt(u32) or (range.high > std.math.maxInt(u32) and range.high != std.math.maxInt(i64))) continue;
            const paths = try a.dupe(ast.Path, operation.paths);
            const edges = try a.dupe(ast.Edge, path.edges);
            edges[0].min_hops = @intCast(range.low);
            edges[0].max_hops = if (range.high == std.math.maxInt(i64)) null else @intCast(range.high);
            paths[0].edges = edges;
            operation.paths = paths;
            for (pending.items) |edit| {
                if (edit.index == index) operation.predicate = edit.residual else result[edit.index].operation.project.value.predicate = edit.residual;
            }
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
