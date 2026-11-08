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
const Origin = union(enum) { length, node, value, constant: v.Value };
const Names = std.StringHashMapUnmanaged(Origin);
fn constant(expression: *const ast.Expr, names: Names, parameters: []const v.Entry) ?v.Value {
    return switch (expression.value) {
        .literal => |literal| switch (literal) {
            .null => .null,
            inline else => |value, tag| @unionInit(v.Value, @tagName(tag), value),
        },
        .variable => |name| if (names.get(name)) |origin| (if (origin == .constant) origin.constant else null) else null,
        .parameter => |name| blk: {
            for (parameters) |entry| if (std.mem.eql(u8, entry.name, name)) break :blk switch (entry.value) {
                .null, .boolean, .integer, .number, .string => entry.value,
                else => null,
            };
            break :blk null;
        },
        .unary => |u| blk: {
            const value = constant(u.operand, names, parameters) orelse return null;
            if (u.op == .positive and (value == .integer or value == .number)) break :blk value;
            if (u.op != .negative) break :blk null;
            break :blk switch (value) {
                .integer => |n| .{ .integer = std.math.negate(n) catch return null },
                .number => |n| .{ .number = -n },
                else => null,
            };
        },
        .binary => |b| blk: {
            const left = constant(b.left, names, parameters) orelse return null;
            const right = constant(b.right, names, parameters) orelse return null;
            if (left != .integer or right != .integer) break :blk null;
            const result = switch (b.op) {
                .add => Form.combine(Form.literal(left.integer), Form.literal(right.integer), .add),
                .subtract => Form.combine(Form.literal(left.integer), Form.literal(right.integer), .subtract),
                .multiply => Form.combine(Form.literal(left.integer), Form.literal(right.integer), .multiply),
                else => null,
            } orelse return null;
            break :blk .{ .integer = result.offset };
        },
        else => null,
    };
}
fn integer(expression: *const ast.Expr, parameters: []const v.Entry) ?i64 {
    const value = constant(expression, .empty, parameters) orelse return null;
    return if (value == .integer) value.integer else null;
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
fn length(expression: *const ast.Expr, names: Names) bool {
    if (expression.value != .variable) return false;
    const origin = names.get(expression.value.variable) orelse return false;
    return origin == .length;
}
fn scalarRead(expression: *const ast.Expr, names: Names, parameters: []const v.Entry) bool {
    if (constant(expression, names, parameters) != null) return true;
    return switch (expression.value) {
        .literal => true,
        .parameter => totalProperty(expression, parameters),
        .property => |property| property.base.value == .variable and (if (names.get(property.base.value.variable)) |origin| origin == .node else false),
        else => false,
    };
}
fn comparison(left: *const ast.Expr, op: ast.Binary, right: *const ast.Expr, names: Names, parameters: []const v.Entry, range: *Range) bool {
    var operation = op;
    switch (op) {
        .eq, .neq, .lt, .lte, .gt, .gte => {},
        else => return false,
    }
    const bound = if (length(left, names)) constant(right, names, parameters) orelse return false else if (length(right, names)) blk: {
        operation = switch (op) {
            .lt => .gt,
            .lte => .gte,
            .gt => .lt,
            .gte => .lte,
            else => op,
        };
        break :blk constant(left, names, parameters) orelse return false;
    } else return false;
    if (bound == .null) {
        // Every comparison is UNKNOWN; the total enclosing predicate cannot
        // accept a row. Keep normal MATCH/OPTIONAL empty-result semantics.
        range.high = -1;
        return true;
    }
    if (operation == .neq or (bound != .integer and bound != .number)) return false;
    if (bound == .number and !std.math.isFinite(bound.number)) return false;
    // Use the runtime's exact mixed numeric ordering. Binary search avoids
    // lossy float-to-integer casts at fractional and i64 boundary values.
    const ge: i128 = if (bound == .integer) @max(0, @as(i128, bound.integer)) else numericBoundary(bound, false);
    const gt: i128 = if (bound == .integer) @max(0, @as(i128, bound.integer) + 1) else numericBoundary(bound, true);
    switch (operation) {
        .eq => {
            if (ge == gt) {
                range.high = -1;
                return true;
            }
            range.low = @max(range.low, ge);
            range.high = @min(range.high, ge);
        },
        .gt => range.low = @max(range.low, gt),
        .gte => range.low = @max(range.low, ge),
        .lt => range.high = @min(range.high, ge - 1),
        .lte => range.high = @min(range.high, gt - 1),
        else => unreachable,
    }
    return true;
}
fn numericBoundary(bound: v.Value, strict: bool) i128 {
    var low: i128 = 0;
    var high: i128 = @as(i128, std.math.maxInt(i64)) + 1;
    while (low < high) {
        const middle = low + @divTrunc(high - low, 2);
        const order = v.Value.compare(.{ .integer = @intCast(middle) }, bound).?;
        if (order == .lt or (strict and order == .eq)) low = middle + 1 else high = middle;
    }
    return low;
}
const Extracted = struct { residual: ?*const ast.Expr = null, observed: bool = false };
fn extract(a: A, expression: *const ast.Expr, names: Names, parameters: []const v.Entry, range: *Range) !?Extracted {
    if (constant(expression, names, parameters)) |value| {
        if (value == .null or (value == .boolean and !value.boolean)) {
            range.high = -1;
            return .{ .observed = true };
        }
        if (value == .boolean) return .{};
    }
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
        const origin: Origin = if (constant(expression, names, parameters)) |value| .{ .constant = value } else if (expression.value == .variable) names.get(expression.value.variable) orelse return null else if (scalarRead(expression, names, parameters)) .value else return null;
        const name = item.alias orelse if (expression.value == .variable) expression.value.variable else return null;
        try next.put(a, name, origin);
    }
    return next;
}
// Prefix projections have already executed before this MATCH. Track their
// constant outputs without claiming that unknown entity/path reads are total
// or allowing a later filter to move across those reads.
fn constantsProjection(a: A, projection: ast.Projection, names: Names, parameters: []const v.Entry) !Names {
    var next: Names = if (projection.wildcard) try names.clone(a) else .empty;
    for (projection.items) |item| {
        const name = item.alias orelse if (item.expression.value == .variable) item.expression.value.variable else continue;
        if (constant(item.expression, names, parameters)) |value| try next.put(a, name, .{ .constant = value }) else _ = next.remove(name);
    }
    return next;
}
fn clauses(a: A, input: []const ast.Clause, parameters: []const v.Entry) anyerror![]const ast.Clause {
    const result = try a.dupe(ast.Clause, input);
    var prefix: Names = .empty;
    for (result, 0..) |*clause, index| switch (clause.operation) {
        .project => |projection| prefix = try constantsProjection(a, projection.value, prefix, parameters),
        .subquery => |inner| {
            clause.operation.subquery = try clauses(a, inner, parameters);
            prefix = .empty;
        },
        .match => |*operation| {
            if (operation.paths.len != 1) continue;
            const path = operation.paths[0];
            if (path.selector != .all or path.edges.len != 1 or path.execution_prefix) continue;
            if (path.execution_summary.length == null and path.edges[0].execution_length == null) continue;
            // Property constraints precede WHERE, including at rejected endpoints.
            // Every boundary crossed by a range must be total, not merely pure.
            if (!totalProperties(path, parameters)) continue;
            var names = try prefix.clone(a);
            prefix = .empty;
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
        else => prefix = .empty,
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
