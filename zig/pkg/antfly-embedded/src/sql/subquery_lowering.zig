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

//! Shape-aware decorrelation. Every inner relation is evaluated a bounded number of times and joined
//! through grouped keys; there is no per-outer-row backend execution.
const std = @import("std");
const ast = @import("ast.zig");
const Allocator = std.mem.Allocator;

pub fn has(node: *const ast.Scalar) bool {
    return switch (node.*) {
        .call => |part| blk: {
            if (part.subquery != null) break :blk true;
            for (part.args) |arg| if (has(arg)) break :blk true;
            break :blk if (part.filter) |filter| has(filter) else false;
        },
        .unary => |part| has(part.operand),
        .binary => |part| has(part.left) or has(part.right),
        .cast => |part| has(part.operand),
        .case_when => |part| blk: {
            for (part.branches) |branch| if (has(branch.condition) or has(branch.value)) break :blk true;
            break :blk if (part.otherwise) |other| has(other) else false;
        },
        .in_list => |part| blk: {
            if (has(part.operand)) break :blk true;
            for (part.values) |value| if (has(value)) break :blk true;
            break :blk false;
        },
        else => false,
    };
}

/// A read in one of these positions is not demanded on every row. Moving it
/// into a grouped/hoisted child could expose runtime cardinality/value errors
/// from a branch SQL would never evaluate. Binding/authorization still cover
/// every branch; masked Apply owns execution demand, not permission bypasses.
pub fn hasConditional(node: *const ast.Scalar) bool {
    return switch (node.*) {
        .literal, .column => false,
        .unary => |part| hasConditional(part.operand),
        .cast => |part| hasConditional(part.operand),
        .binary => |part| hasConditional(part.left) or (if (part.op == .@"and" or part.op == .@"or") has(part.right) else hasConditional(part.right)),
        .in_list => |part| blk: {
            if (hasConditional(part.operand)) break :blk true;
            for (part.values) |value| if (has(value)) break :blk true;
            break :blk false;
        },
        .case_when => |part| blk: {
            for (part.branches, 0..) |branch, index| {
                if ((if (index == 0) hasConditional(branch.condition) else has(branch.condition)) or has(branch.value)) break :blk true;
            }
            break :blk if (part.otherwise) |otherwise| has(otherwise) else false;
        },
        .call => |part| blk: {
            if (part.subquery != null) break :blk false;
            if (std.mem.eql(u8, part.name, "coalesce")) {
                for (part.args, 0..) |arg, index| if (if (index == 0) hasConditional(arg) else has(arg)) break :blk true;
            } else for (part.args) |arg| if (hasConditional(arg)) break :blk true;
            break :blk if (part.filter) |filter| has(filter) else false;
        },
    };
}
pub fn predicateHas(predicate: *const ast.Predicate) bool {
    return switch (predicate.*) {
        .scalar => |value| has(value),
        .conjunction, .disjunction => |part| predicateHas(part.left) or predicateHas(part.right),
        .negation => |part| predicateHas(part),
        else => false,
    };
}
pub fn accepts(statement: ast.Select) bool {
    for (statement.values_arms) |arm| if (accepts(arm.*)) return true;
    if (statement.set_operation) |set| {
        if (accepts(set.left.*) or accepts(set.right.*)) return true;
    }
    for (statement.columns) |column| if (column.expression) |value| if (has(value)) return true;
    if (statement.predicate) |predicate| if (predicateHas(predicate)) return true;
    for (statement.group_by) |value| if (has(value)) return true;
    if (statement.having) |value| if (has(value)) return true;
    for (statement.order_by) |order| if (order.expression) |value| if (has(value)) return true;
    return false;
}
const Names = std.StringHashMapUnmanaged(void);
fn aliases(alloc: Allocator, relation: *const ast.Relation, names: *Names) !void {
    switch (relation.*) {
        .table => |table| try names.put(alloc, table.alias orelse table.name.table, {}),
        .derived => |query| if (query.preserve_scope) try aliases(alloc, query.query.source.?, names) else try names.put(alloc, query.alias, {}),
        .join => |join| {
            try aliases(alloc, join.left, names);
            try aliases(alloc, join.right, names);
        },
    }
}
const Key = struct { inner: *const ast.Scalar, outer: *const ast.Scalar, comparison: ast.Scalar.Binary = .eq };
const Builder = struct {
    alloc: Allocator,
    source: *const ast.Relation,
    outer: Names = .empty,
    serial: usize = 0,
    row_producers: std.ArrayList(struct { query: *const ast.Select, demand: ?*const ast.Scalar, alias: []const u8 }) = .empty,
    fn scalar(self: *Builder, value: ast.Scalar) !*const ast.Scalar {
        const out = try self.alloc.create(ast.Scalar);
        out.* = value;
        return out;
    }
    fn relation(self: *Builder, value: ast.Relation) !*const ast.Relation {
        const out = try self.alloc.create(ast.Relation);
        out.* = value;
        return out;
    }
    fn field(self: *Builder, qualifier: []const u8, name: []const u8) !*const ast.Scalar {
        return self.scalar(.{ .column = try std.fmt.allocPrint(self.alloc, "{s}\x00{s}", .{ qualifier, name }) });
    }
    fn call(self: *Builder, name: []const u8, args: []const *const ast.Scalar) !*const ast.Scalar {
        return self.scalar(.{ .call = .{ .name = name, .args = try self.alloc.dupe(*const ast.Scalar, args) } });
    }
    fn groupExpression(self: *Builder, value: *const ast.Scalar) !*const ast.Scalar {
        // A generated integer constant is a value, never SQL GROUP BY n's
        // positional shorthand. Preserve that distinction through binding.
        if (value.* == .literal and value.literal == .integer)
            return self.scalar(.{ .cast = .{ .operand = value, .type = .integer } });
        return value;
    }
    fn outerRef(self: *Builder, value: *const ast.Scalar, local: Names) bool {
        if (value.* != .column) return false;
        const separator = std.mem.indexOfScalar(u8, value.column, 0) orelse return false;
        const qualifier = value.column[0..separator];
        return !local.contains(qualifier) and self.outer.contains(qualifier);
    }
    const NestedScope = struct {
        parent: ?*const NestedScope = null,
        source: ?*const ast.Relation = null,
        table: ?ast.Name = null,
        local: ?*const Names = null,
        /// A closure proof cannot assume that an unqualified name belongs to
        /// a table whose layout has not been pinned yet.
        conservative: bool = false,
    };
    fn relationDefines(value: *const ast.Relation, qualifier: []const u8) bool {
        return switch (value.*) {
            .table => |part| std.mem.eql(u8, part.alias orelse part.name.table, qualifier),
            .derived => |part| std.mem.eql(u8, part.alias, qualifier),
            .join => |part| relationDefines(part.left, qualifier) or relationDefines(part.right, qualifier),
        };
    }
    fn nestedScopeDefines(scope: ?*const NestedScope, qualifier: []const u8) bool {
        var current = scope;
        while (current) |node| : (current = node.parent) {
            if (node.local) |names| if (names.contains(qualifier)) return true;
            if (node.source) |source| if (relationDefines(source, qualifier)) return true;
            if (node.table) |table| if (std.mem.eql(u8, table.table, qualifier)) return true;
        }
        return false;
    }
    fn nestedOuterField(self: *Builder, name: []const u8, scope: ?*const NestedScope) bool {
        if (name.len == 0) return false;
        const conservative = if (scope) |frame| frame.conservative else false;
        const separator = std.mem.indexOfScalar(u8, name, 0) orelse return conservative;
        const qualifier = name[0..separator];
        return !nestedScopeDefines(scope, qualifier) and (conservative or self.outer.contains(qualifier));
    }
    /// A nested value relation is safe to leave inside a derived child only
    /// when it cannot reach past that child's lexical parent. The child will
    /// lower its own subqueries after binding, including correlations to the
    /// current inner relation. Qualified references to this builder's outer
    /// scope must not be smuggled through that independent boundary.
    fn nestedOuterScalar(self: *Builder, value: *const ast.Scalar, scope: ?*const NestedScope) bool {
        return switch (value.*) {
            .column => |name| self.nestedOuterField(name, scope),
            .unary => |part| self.nestedOuterScalar(part.operand, scope),
            .binary => |part| self.nestedOuterScalar(part.left, scope) or self.nestedOuterScalar(part.right, scope),
            .cast => |part| self.nestedOuterScalar(part.operand, scope),
            .case_when => |part| blk: {
                for (part.branches) |branch| if (self.nestedOuterScalar(branch.condition, scope) or self.nestedOuterScalar(branch.value, scope)) break :blk true;
                break :blk if (part.otherwise) |other| self.nestedOuterScalar(other, scope) else false;
            },
            .in_list => |part| blk: {
                if (self.nestedOuterScalar(part.operand, scope)) break :blk true;
                for (part.values) |item| if (self.nestedOuterScalar(item, scope)) break :blk true;
                break :blk false;
            },
            .call => |part| blk: {
                for (part.args) |arg| if (self.nestedOuterScalar(arg, scope)) break :blk true;
                if (part.filter) |filter| if (self.nestedOuterScalar(filter, scope)) break :blk true;
                if (part.window) |window| {
                    for (window.partition) |item| if (self.nestedOuterScalar(item, scope)) break :blk true;
                    for (window.order) |order| {
                        if (self.nestedOuterField(order.field, scope)) break :blk true;
                        if (order.expression) |item| if (self.nestedOuterScalar(item, scope)) break :blk true;
                    }
                }
                break :blk if (part.subquery) |query| self.nestedOuterSelect(query, scope) else false;
            },
            else => false,
        };
    }
    fn nestedOuterPredicate(self: *Builder, value: *const ast.Predicate, scope: ?*const NestedScope) bool {
        return switch (value.*) {
            .scalar => |scalar_value| self.nestedOuterScalar(scalar_value, scope),
            .comparison => |part| self.nestedOuterField(part.field, scope),
            .is_null => |part| self.nestedOuterField(part.field, scope),
            .conjunction, .disjunction => |part| self.nestedOuterPredicate(part.left, scope) or self.nestedOuterPredicate(part.right, scope),
            .negation => |part| self.nestedOuterPredicate(part, scope),
        };
    }
    fn nestedOuterRelation(self: *Builder, value: *const ast.Relation, scope: ?*const NestedScope) bool {
        return switch (value.*) {
            .table => false,
            .derived => |part| self.nestedOuterSelect(part.query, scope),
            .join => |part| self.nestedOuterRelation(part.left, scope) or self.nestedOuterRelation(part.right, scope) or
                (if (part.condition) |condition| self.nestedOuterScalar(condition, scope) else false) or
                (if (part.demand) |demand| self.nestedOuterScalar(demand, scope) else false),
        };
    }
    fn nestedOuterSelect(self: *Builder, query: *const ast.Select, parent: ?*const NestedScope) bool {
        const scope: NestedScope = .{ .parent = parent, .source = query.source, .table = query.table, .conservative = if (parent) |frame| frame.conservative else false };
        for (query.values_arms) |arm| if (self.nestedOuterSelect(arm, &scope)) return true;
        if (query.set_operation) |set| if (self.nestedOuterSelect(set.left, &scope) or self.nestedOuterSelect(set.right, &scope)) return true;
        if (query.source) |source| if (self.nestedOuterRelation(source, &scope)) return true;
        for (query.ctes) |cte| if (self.nestedOuterSelect(cte.query, &scope)) return true;
        for (query.columns) |column| {
            if (self.nestedOuterField(column.field, &scope)) return true;
            if (column.expression) |value| if (self.nestedOuterScalar(value, &scope)) return true;
        }
        if (query.predicate) |predicate| if (self.nestedOuterPredicate(predicate, &scope)) return true;
        for (query.group_by) |value| if (self.nestedOuterScalar(value, &scope)) return true;
        if (query.having) |value| if (self.nestedOuterScalar(value, &scope)) return true;
        for (query.order_by) |order| {
            if (self.nestedOuterField(order.field, &scope)) return true;
            if (order.expression) |value| if (self.nestedOuterScalar(value, &scope)) return true;
        }
        for (query.windows) |named| {
            for (named.window.partition) |value| if (self.nestedOuterScalar(value, &scope)) return true;
            for (named.window.order) |order| {
                if (self.nestedOuterField(order.field, &scope)) return true;
                if (order.expression) |value| if (self.nestedOuterScalar(value, &scope)) return true;
            }
        }
        return false;
    }
    fn referencesOuter(self: *Builder, value: *const ast.Scalar, local: Names) bool {
        return self.referenceSides(value, local) & 2 != 0;
    }
    /// Unqualified names stay in the inner binding domain. Only expressions
    /// whose column references all belong to the outer scope form outer keys.
    fn referenceSides(self: *Builder, value: *const ast.Scalar, local: Names) u2 {
        return switch (value.*) {
            .column => if (self.outerRef(value, local)) 2 else 1,
            .call => |part| blk: {
                const scope: NestedScope = .{ .local = &local };
                if (part.window != null or if (part.subquery) |query| self.nestedOuterSelect(query, &scope) else false) break :blk 3;
                var sides: u2 = if (part.subquery != null) 1 else 0;
                for (part.args) |arg| sides |= self.referenceSides(arg, local);
                if (part.filter) |filter| sides |= self.referenceSides(filter, local);
                break :blk sides;
            },
            .unary => |part| self.referenceSides(part.operand, local),
            .binary => |part| self.referenceSides(part.left, local) | self.referenceSides(part.right, local),
            .cast => |part| self.referenceSides(part.operand, local),
            .case_when => |part| blk: {
                var sides: u2 = 0;
                for (part.branches) |branch| sides |= self.referenceSides(branch.condition, local) | self.referenceSides(branch.value, local);
                if (part.otherwise) |other| sides |= self.referenceSides(other, local);
                break :blk sides;
            },
            .in_list => |part| blk: {
                var sides = self.referenceSides(part.operand, local);
                for (part.values) |item| sides |= self.referenceSides(item, local);
                break :blk sides;
            },
            else => 0,
        };
    }
    fn predicateScalar(self: *Builder, input: *const ast.Predicate) anyerror!*const ast.Scalar {
        return switch (input.*) {
            .scalar => |value| value,
            .comparison => |part| self.scalar(.{ .binary = .{ .op = @fromBackingInt(@backingInt(ast.Scalar.Binary.eq) + @backingInt(part.op)), .left = try self.scalar(.{ .column = part.field }), .right = try self.scalar(.{ .literal = part.value }) } }),
            .is_null => |part| self.scalar(.{ .unary = .{ .op = if (part.negated) .is_not_null else .is_null, .operand = try self.scalar(.{ .column = part.field }) } }),
            .negation => |part| self.scalar(.{ .unary = .{ .op = .not, .operand = try self.predicateScalar(part) } }),
            .conjunction, .disjunction => |part| self.scalar(.{ .binary = .{ .op = if (input.* == .conjunction) .@"and" else .@"or", .left = try self.predicateScalar(part.left), .right = try self.predicateScalar(part.right) } }),
        };
    }
    fn hasOuterOr(self: *Builder, value: *const ast.Scalar, local: Names) bool {
        if (value.* != .binary) return false;
        const part = value.binary;
        return switch (part.op) {
            .@"or" => self.referencesOuter(value, local),
            .@"and" => self.hasOuterOr(part.left, local) or self.hasOuterOr(part.right, local),
            else => false,
        };
    }
    /// EXISTS distributes over OR. Keep local-only disjunctions intact so
    /// ordinary filters do not multiply inner scans. Bound the distributive
    /// expansion before adding any derived relations to the outer source.
    fn existenceBranches(self: *Builder, value: *const ast.Scalar, local: Names) anyerror!std.ArrayList(*const ast.Scalar) {
        var result: std.ArrayList(*const ast.Scalar) = .empty;
        if (value.* != .binary or (value.binary.op != .@"and" and value.binary.op != .@"or") or
            (value.binary.op == .@"or" and !self.referencesOuter(value, local)))
        {
            try result.append(self.alloc, value);
            return result;
        }
        const left = try self.existenceBranches(value.binary.left, local);
        const right = try self.existenceBranches(value.binary.right, local);
        if (value.binary.op == .@"or") {
            if (left.items.len + right.items.len > 8) return error.SqlProgramLimitExceeded;
            try result.appendSlice(self.alloc, left.items);
            try result.appendSlice(self.alloc, right.items);
            return result;
        }
        if (left.items.len > 8 / right.items.len) return error.SqlProgramLimitExceeded;
        for (left.items) |lhs| for (right.items) |rhs| {
            try result.append(self.alloc, try self.scalar(.{ .binary = .{ .op = .@"and", .left = lhs, .right = rhs } }));
        };
        return result;
    }
    pub fn extract(self: *Builder, value: *const ast.Scalar, local: Names, keys: *std.ArrayList(Key), range: ?*?Key) anyerror!?*const ast.Scalar {
        if (value.* == .binary and value.binary.op == .@"and") {
            const left = try self.extract(value.binary.left, local, keys, range);
            const right = try self.extract(value.binary.right, local, keys, range);
            return if (left != null and right != null) try self.scalar(.{ .binary = .{ .op = .@"and", .left = left.?, .right = right.? } }) else left orelse right;
        }
        if (value.* == .binary and value.binary.op == .eq) {
            const part = value.binary;
            if (self.referenceSides(part.left, local) == 2 and !self.referencesOuter(part.right, local)) {
                try keys.append(self.alloc, .{ .inner = part.right, .outer = part.left });
                return null;
            }
            if (self.referenceSides(part.right, local) == 2 and !self.referencesOuter(part.left, local)) {
                try keys.append(self.alloc, .{ .inner = part.left, .outer = part.right });
                return null;
            }
        }
        if (range) |out| if (value.* == .binary) {
            const part = value.binary;
            const reverse: ast.Scalar.Binary = switch (part.op) {
                .lt => .gt,
                .lte => .gte,
                .gt => .lt,
                .gte => .lte,
                else => {
                    if (self.referencesOuter(value, local)) return error.UnsupportedSqlShape;
                    return value;
                },
            };
            const key: ?Key = if (self.referenceSides(part.left, local) == 2 and !self.referencesOuter(part.right, local))
                .{ .outer = part.left, .inner = part.right, .comparison = part.op }
            else if (self.referenceSides(part.right, local) == 2 and !self.referencesOuter(part.left, local))
                .{ .outer = part.right, .inner = part.left, .comparison = reverse }
            else
                null;
            if (key) |candidate| {
                if (out.* != null) return error.UnsupportedSqlShape;
                out.* = candidate;
                return null;
            }
        };
        if (self.referencesOuter(value, local)) return error.UnsupportedSqlShape;
        return value;
    }
    fn subquery(self: *Builder, expression: *const ast.Scalar) anyerror!*const ast.Scalar {
        if (std.mem.eql(u8, expression.call.name, "$in_subquery")) return self.membership(expression);
        if (std.mem.startsWith(u8, expression.call.name, "$any_") or std.mem.startsWith(u8, expression.call.name, "$all_")) return self.quantified(expression);
        const exists = std.mem.eql(u8, expression.call.name, "$exists");
        const original = if (exists) expression.call.subquery.? else try self.valueQuery(expression.call.subquery.?, false);
        if (exists and (original.count_all or @import("aggregate_binding.zig").accepts(original.*))) return error.UnsupportedSqlShape;
        if (original.set_operation != null or original.ctes.len != 0 or original.order_by.len != 0 or original.limit != null or original.offset != null or original.group_by.len != 0 or original.having != null or @import("window_binding.zig").accepts(original.*)) return error.UnsupportedSqlShape;
        if (!exists and original.columns.len != 1 and !original.count_all) return error.InvalidSqlParameters;
        var local: Names = .empty;
        if (original.source) |source| try aliases(self.alloc, source, &local) else if (original.table) |table| try local.put(self.alloc, table.table, {});
        if (exists) if (original.predicate) |predicate| {
            const filter = try self.predicateScalar(predicate);
            if (self.hasOuterOr(filter, local)) {
                const branches = try self.existenceBranches(filter, local);
                var combined: ?*const ast.Scalar = null;
                for (branches.items) |branch| {
                    const branch_predicate = try self.alloc.create(ast.Predicate);
                    branch_predicate.* = .{ .scalar = branch };
                    const branch_query = try self.alloc.create(ast.Select);
                    branch_query.* = original.*;
                    branch_query.predicate = branch_predicate;
                    const branch_expression = try self.scalar(.{ .call = .{ .name = "$exists", .args = &.{}, .subquery = branch_query } });
                    const witness = try self.subquery(branch_expression);
                    combined = if (combined) |prior| try self.scalar(.{ .binary = .{ .op = .@"or", .left = prior, .right = witness } }) else witness;
                }
                return combined.?;
            }
        };
        var keys: std.ArrayList(Key) = .empty;
        var range: ?Key = null;
        const residual = if (original.predicate) |predicate| try self.extract(try self.predicateScalar(predicate), local, &keys, if (exists) &range else null) else null;
        var query = original.*;
        query.order_by = &.{};
        query.count_all = false;
        query.count_alias = null;
        query.predicate = if (residual) |value| blk: {
            const out = try self.alloc.create(ast.Predicate);
            out.* = .{ .scalar = value };
            break :blk out;
        } else null;
        // Validate discarded projections in the inner binding domain without
        // evaluating them or retaining cold storage projection dependencies.
        const probes = try self.alloc.alloc(*const ast.Scalar, if (exists) original.columns.len else 0);
        for (probes, 0..) |*probe, index| {
            const projection = original.columns[index];
            probe.* = projection.expression orelse try self.scalar(.{ .column = projection.field });
            if (self.referencesOuter(probe.*, local)) return error.UnsupportedSqlShape;
        }
        const columns = try self.alloc.alloc(ast.Projection, keys.items.len + 2);
        const groups = try self.alloc.alloc(*const ast.Scalar, keys.items.len);
        const alias = try std.fmt.allocPrint(self.alloc, "$subquery_{d}", .{self.serial});
        self.serial += 1;
        if (self.serial > 64 or self.outer.contains(alias)) return error.SqlProgramLimitExceeded;
        try self.outer.put(self.alloc, alias, {});
        var condition: ?*const ast.Scalar = null;
        for (keys.items, columns[0..keys.items.len], groups, 0..) |key, *column, *group, index| {
            const name = try std.fmt.allocPrint(self.alloc, "$key_{d}", .{index});
            column.* = .{ .alias = name, .expression = key.inner };
            group.* = try self.groupExpression(key.inner);
            const equal = try self.scalar(.{ .binary = .{ .op = .eq, .left = key.outer, .right = try self.field(alias, name) } });
            condition = if (condition) |prior| try self.scalar(.{ .binary = .{ .op = .@"and", .left = prior, .right = equal } }) else equal;
        }
        // Validate all discarded columns in one domain, then compile to the
        // existing existence count. No per-projection aggregate state/work.
        const count = if (probes.len != 0)
            try self.call("count", &.{try self.call("$validate", probes)})
        else
            try self.scalar(.{ .call = .{ .name = "count", .args = &.{}, .star = true } });
        columns[keys.items.len] = .{ .alias = "$count", .expression = count };
        var value: *const ast.Scalar = if (range) |key|
            try self.call(if (key.comparison == .gt or key.comparison == .gte) "min" else "max", &.{key.inner})
        else
            try self.scalar(.{ .literal = .{ .integer = 1 } });
        var aggregate_result: ?*const ast.Scalar = null;
        var aggregate_columns: std.ArrayList(ast.Projection) = .empty;
        if (!exists) {
            value = if (original.count_all) count else original.columns[0].expression orelse try self.scalar(.{ .column = original.columns[0].field });
            if (self.referencesOuter(value, local)) return error.UnsupportedSqlShape;
            if (@import("aggregate_binding.zig").contains(value)) {
                aggregate_result = try self.aggregateResult(value, alias, &aggregate_columns);
                value = try self.scalar(.{ .literal = .{ .integer = 1 } });
            } else value = try self.call("min", &.{value});
        }
        columns[keys.items.len + 1] = .{ .alias = "$value", .expression = value };
        const combined = try self.alloc.alloc(ast.Projection, columns.len + aggregate_columns.items.len);
        @memcpy(combined[0..columns.len], columns);
        @memcpy(combined[columns.len..], aggregate_columns.items);
        query.columns = combined;
        query.group_by = groups;
        if (exists and keys.items.len == 0 and range == null) {
            // An uncorrelated existence test needs at most one matching row.
            // Keep validation in the child's original name/type domain and
            // let the pull iterator stop after its first qualifying page.
            // Correlated groups/range summaries must still examine all rows.
            const first = try self.alloc.create(ast.Select);
            first.* = original.*;
            first.columns = try self.alloc.dupe(ast.Projection, &.{.{ .alias = "$exists_value", .expression = if (probes.len == 0) try self.scalar(.{ .literal = .{ .integer = 1 } }) else try self.call("$validate", probes) }});
            first.limit = .{ .integer = 1 };
            columns[0].expression = try self.scalar(.{ .call = .{ .name = "count", .args = &.{}, .star = true } });
            // combined owns a copy, so replace its count as well.
            combined[0] = columns[0];
            query.table = null;
            query.source = try self.relation(.{ .derived = .{ .query = first, .alias = "$exists_source" } });
            query.predicate = null;
        }
        const owned = try self.alloc.create(ast.Select);
        owned.* = query;
        self.source = try self.relation(.{ .join = .{ .kind = .left, .left = self.source, .right = try self.relation(.{ .derived = .{ .query = owned, .alias = alias, .hidden = true } }), .condition = condition } });
        const observed = try self.field(alias, "$count");
        if (range) |key| return self.call("coalesce", &.{
            try self.scalar(.{ .binary = .{ .op = key.comparison, .left = key.outer, .right = try self.field(alias, "$value") } }),
            try self.scalar(.{ .literal = .{ .boolean = false } }),
        });
        if (exists) return self.scalar(.{ .binary = .{ .op = .gt, .left = try self.call("coalesce", &.{ observed, try self.scalar(.{ .literal = .{ .integer = 0 } }) }), .right = try self.scalar(.{ .literal = .{ .integer = 0 } }) } });
        const result = try self.field(alias, "$value");
        if (aggregate_result) |aggregate| return aggregate;
        return self.call("$single", &.{ result, observed });
    }

    /// A complete uncorrelated value relation is a normal derived query, not
    /// a per-row Apply. Preserve its ordering, limit, grouping, set semantics,
    /// and output names inside the boundary, then assign a positional name.
    /// Binding that child independently rejects outer references: correlated
    /// top-K/group/window semantics must not be silently changed by hoisting.
    fn valueQuery(self: *Builder, original: *const ast.Select, wrap_aggregate: bool) !*const ast.Select {
        if (original.set_operation == null and original.ctes.len == 0 and original.order_by.len == 0 and original.limit == null and original.offset == null and original.group_by.len == 0 and original.having == null and !@import("window_binding.zig").accepts(original.*) and !(wrap_aggregate and (original.count_all or @import("aggregate_binding.zig").accepts(original.*)))) return original;
        const wrapped = try self.alloc.create(ast.Select);
        wrapped.* = .{
            .source = try self.relation(.{ .derived = .{ .query = original, .alias = "$value_source", .columns = &.{"$value"} } }),
            .columns = try self.alloc.dupe(ast.Projection, &.{.{ .expression = try self.field("$value_source", "$value") }}),
        };
        return wrapped;
    }

    /// Reconstruct a global aggregate projection after joining grouped inner
    /// states. Missing correlation groups mean COUNT=0 and other aggregates
    /// NULL; evaluating the original scalar expression here preserves that
    /// empty-input contract even for CASE, arithmetic and COALESCE.
    fn aggregateResult(self: *Builder, input: *const ast.Scalar, alias: []const u8, columns: *std.ArrayList(ast.Projection)) anyerror!*const ast.Scalar {
        if (input.* == .call and (input.call.within_group != null or @import("aggregate_binding.zig").aggregateKind(input.call.name) != null)) {
            if (columns.items.len >= 256) return error.SqlProgramLimitExceeded;
            const name = try std.fmt.allocPrint(self.alloc, "$aggregate_{d}", .{columns.items.len});
            try columns.append(self.alloc, .{ .alias = name, .expression = input });
            const result = try self.field(alias, name);
            return if (std.mem.eql(u8, input.call.name, "count")) self.call("coalesce", &.{ result, try self.scalar(.{ .literal = .{ .integer = 0 } }) }) else result;
        }
        return self.scalar(switch (input.*) {
            .literal => input.*,
            .column => return error.SqlGroupingError,
            .unary => |part| .{ .unary = .{ .op = part.op, .operand = try self.aggregateResult(part.operand, alias, columns) } },
            .binary => |part| .{ .binary = .{ .op = part.op, .left = try self.aggregateResult(part.left, alias, columns), .right = try self.aggregateResult(part.right, alias, columns) } },
            .cast => |part| .{ .cast = .{ .type = part.type, .element_type = part.element_type, .operand = try self.aggregateResult(part.operand, alias, columns) } },
            .call => |part| blk: {
                if (part.subquery != null or part.window != null or part.filter != null or part.distinct or part.star) return error.UnsupportedSqlShape;
                var copy = part;
                const args = try self.alloc.alloc(*const ast.Scalar, part.args.len);
                for (part.args, args) |arg, *out| out.* = try self.aggregateResult(arg, alias, columns);
                copy.args = args;
                break :blk .{ .call = copy };
            },
            .case_when => |part| blk: {
                const branches = try self.alloc.alloc(ast.Scalar.Branch, part.branches.len);
                for (part.branches, branches) |branch, *out| out.* = .{ .condition = try self.aggregateResult(branch.condition, alias, columns), .value = try self.aggregateResult(branch.value, alias, columns) };
                break :blk .{ .case_when = .{ .branches = branches, .otherwise = if (part.otherwise) |other| try self.aggregateResult(other, alias, columns) else null } };
            },
            .in_list => |part| blk: {
                const values = try self.alloc.alloc(*const ast.Scalar, part.values.len);
                for (part.values, values) |item, *out| out.* = try self.aggregateResult(item, alias, columns);
                break :blk .{ .in_list = .{ .operand = try self.aggregateResult(part.operand, alias, columns), .values = values, .negated = part.negated } };
            },
        });
    }
    /// Ordered quantifiers use one grouped summary per correlation key. MIN
    /// and MAX preserve the executor's typed ordering; counts distinguish
    /// empty sets from all-NULL sets. Equality ANY shares hash membership.
    fn quantified(self: *Builder, expression: *const ast.Scalar) anyerror!*const ast.Scalar {
        const every = std.mem.startsWith(u8, expression.call.name, "$all_");
        const suffix = expression.call.name[5..];
        if (std.mem.eql(u8, suffix, "like") or std.mem.eql(u8, suffix, "ilike") or std.mem.eql(u8, suffix, "not_like") or std.mem.eql(u8, suffix, "not_ilike"))
            return self.patternQuantified(expression, every, std.mem.endsWith(u8, suffix, "ilike"), std.mem.startsWith(u8, suffix, "not_"));
        const op = std.meta.stringToEnum(ast.Scalar.Binary, suffix) orelse return error.UnsupportedSqlShape;
        if (try self.quantifiedNeedsApply(expression.call.subquery.?)) return self.quantifiedProducer(expression, op, every, false);
        if ((!every and op == .eq) or (every and op == .neq)) {
            const result = try self.membership(expression);
            return if (every) self.scalar(.{ .unary = .{ .op = .not, .operand = result } }) else result;
        }
        const original = try self.valueQuery(expression.call.subquery.?, true);
        if (expression.call.args.len != 1 or original.columns.len != 1 or original.count_all) return error.InvalidSqlParameters;
        if (original.set_operation != null or original.ctes.len != 0 or original.order_by.len != 0 or original.limit != null or original.offset != null or original.group_by.len != 0 or original.having != null or @import("aggregate_binding.zig").accepts(original.*) or @import("window_binding.zig").accepts(original.*)) return error.UnsupportedSqlShape;
        var local: Names = .empty;
        if (original.source) |source| try aliases(self.alloc, source, &local) else if (original.table) |table| try local.put(self.alloc, table.table, {});
        const value = original.columns[0].expression orelse try self.scalar(.{ .column = original.columns[0].field });
        if (self.referencesOuter(value, local)) return error.UnsupportedSqlShape;
        const operand = try self.rewrite(expression.call.args[0]);
        var keys: std.ArrayList(Key) = .empty;
        const residual = if (original.predicate) |predicate| try self.extract(try self.predicateScalar(predicate), local, &keys, null) else null;
        var query = original.*;
        query.predicate = if (residual) |predicate| blk: {
            const out = try self.alloc.create(ast.Predicate);
            out.* = .{ .scalar = predicate };
            break :blk out;
        } else null;
        const alias = try std.fmt.allocPrint(self.alloc, "$subquery_{d}", .{self.serial});
        self.serial += 1;
        if (self.serial > 64 or self.outer.contains(alias)) return error.SqlProgramLimitExceeded;
        try self.outer.put(self.alloc, alias, {});
        const both = op == .eq or op == .neq;
        const minimum = switch (op) {
            .eq, .neq => true,
            .lt, .lte => every,
            .gt, .gte => !every,
            else => return error.UnsupportedSqlShape,
        };
        const columns = try self.alloc.alloc(ast.Projection, keys.items.len + 3 + @intFromBool(both));
        const groups = try self.alloc.alloc(*const ast.Scalar, keys.items.len);
        var condition: ?*const ast.Scalar = null;
        for (keys.items, 0..) |key, index| {
            const name = try std.fmt.allocPrint(self.alloc, "$key_{d}", .{index});
            columns[index] = .{ .alias = name, .expression = key.inner };
            groups[index] = try self.groupExpression(key.inner);
            const equal = try self.scalar(.{ .binary = .{ .op = .eq, .left = key.outer, .right = try self.field(alias, name) } });
            condition = if (condition) |prior| try self.scalar(.{ .binary = .{ .op = .@"and", .left = prior, .right = equal } }) else equal;
        }
        columns[keys.items.len] = .{ .alias = "$count", .expression = try self.scalar(.{ .call = .{ .name = "count", .args = &.{}, .star = true } }) };
        columns[keys.items.len + 1] = .{ .alias = "$nonnull", .expression = try self.call("count", &.{value}) };
        columns[keys.items.len + 2] = .{ .alias = "$bound", .expression = try self.call(if (minimum) "min" else "max", &.{value}) };
        if (both) columns[keys.items.len + 3] = .{ .alias = "$upper", .expression = try self.call("max", &.{value}) };
        query.columns = columns;
        query.group_by = groups;
        const owned = try self.alloc.create(ast.Select);
        owned.* = query;
        self.source = try self.relation(.{ .join = .{ .kind = .left, .left = self.source, .right = try self.relation(.{ .derived = .{ .query = owned, .alias = alias, .hidden = true } }), .condition = condition } });
        var comparison = try self.scalar(.{ .binary = .{ .op = op, .left = operand, .right = try self.field(alias, "$bound") } });
        if (both) comparison = try self.scalar(.{ .binary = .{
            .op = if (every) .@"and" else .@"or",
            .left = comparison,
            .right = try self.scalar(.{ .binary = .{ .op = op, .left = operand, .right = try self.field(alias, "$upper") } }),
        } });
        const zero = try self.scalar(.{ .literal = .{ .integer = 0 } });
        const total = try self.call("coalesce", &.{ try self.field(alias, "$count"), zero });
        const nonnull = try self.call("coalesce", &.{ try self.field(alias, "$nonnull"), zero });
        const branches = try self.alloc.alloc(ast.Scalar.Branch, 3);
        branches[0] = .{ .condition = try self.scalar(.{ .binary = .{ .op = .eq, .left = total, .right = zero } }), .value = try self.scalar(.{ .literal = .{ .boolean = every } }) };
        branches[1] = .{ .condition = try self.scalar(.{ .unary = .{ .op = if (every) .is_false else .is_true, .operand = comparison } }), .value = try self.scalar(.{ .literal = .{ .boolean = !every } }) };
        branches[2] = .{ .condition = try self.scalar(.{ .binary = .{ .op = .@"or", .left = try self.scalar(.{ .unary = .{ .op = .is_null, .operand = operand } }), .right = try self.scalar(.{ .binary = .{ .op = .gt, .left = total, .right = nonnull } }) } }), .value = try self.scalar(.{ .cast = .{ .type = .boolean, .operand = try self.scalar(.{ .literal = .null }) } }) };
        return self.scalar(.{ .case_when = .{ .branches = branches, .otherwise = try self.scalar(.{ .literal = .{ .boolean = every } }) } });
    }

    fn patternQuantified(self: *Builder, expression: *const ast.Scalar, every: bool, insensitive: bool, negated: bool) anyerror!*const ast.Scalar {
        if (try self.quantifiedNeedsApply(expression.call.subquery.?)) return self.quantifiedProducer(expression, if (insensitive) .ilike else .like, every, negated);
        const original = try self.valueQuery(expression.call.subquery.?, true);
        if (expression.call.args.len != 1 or original.columns.len != 1 or original.count_all) return error.InvalidSqlParameters;
        if (original.set_operation != null or original.ctes.len != 0 or original.order_by.len != 0 or original.limit != null or original.offset != null or original.group_by.len != 0 or original.having != null or @import("aggregate_binding.zig").accepts(original.*) or @import("window_binding.zig").accepts(original.*)) return error.UnsupportedSqlShape;
        var local: Names = .empty;
        if (original.source) |source| try aliases(self.alloc, source, &local) else if (original.table) |table| try local.put(self.alloc, table.table, {});
        const value = original.columns[0].expression orelse try self.scalar(.{ .column = original.columns[0].field });
        if (self.referencesOuter(value, local)) return error.UnsupportedSqlShape;
        const operand = try self.rewrite(expression.call.args[0]);
        var keys: std.ArrayList(Key) = .empty;
        const residual = if (original.predicate) |predicate| try self.extract(try self.predicateScalar(predicate), local, &keys, null) else null;
        var query = original.*;
        query.predicate = if (residual) |predicate| blk: {
            const out = try self.alloc.create(ast.Predicate);
            out.* = .{ .scalar = predicate };
            break :blk out;
        } else null;
        const alias = try std.fmt.allocPrint(self.alloc, "$subquery_{d}", .{self.serial});
        self.serial += 1;
        if (self.serial > 64 or self.outer.contains(alias)) return error.SqlProgramLimitExceeded;
        try self.outer.put(self.alloc, alias, {});
        const columns = try self.alloc.alloc(ast.Projection, keys.items.len + 1);
        const groups = try self.alloc.alloc(*const ast.Scalar, keys.items.len);
        var condition: ?*const ast.Scalar = null;
        for (keys.items, 0..) |key, index| {
            const name = try std.fmt.allocPrint(self.alloc, "$key_{d}", .{index});
            columns[index] = .{ .alias = name, .expression = key.inner };
            groups[index] = try self.groupExpression(key.inner);
            const equal = try self.scalar(.{ .binary = .{ .op = .eq, .left = key.outer, .right = try self.field(alias, name) } });
            condition = if (condition) |prior| try self.scalar(.{ .binary = .{ .op = .@"and", .left = prior, .right = equal } }) else equal;
        }
        columns[keys.items.len] = .{ .alias = "$patterns", .expression = try self.scalar(.{ .call = .{ .name = "$pattern_set", .args = try self.alloc.dupe(*const ast.Scalar, &.{value}), .distinct = true } }) };
        query.columns = columns;
        query.group_by = groups;
        const owned = try self.alloc.create(ast.Select);
        owned.* = query;
        self.source = try self.relation(.{ .join = .{ .kind = .left, .left = self.source, .right = try self.relation(.{ .derived = .{ .query = owned, .alias = alias, .hidden = true } }), .condition = condition } });
        return self.call("$pattern_quantified", &.{
            operand,
            try self.field(alias, "$patterns"),
            try self.scalar(.{ .literal = .{ .boolean = every } }),
            try self.scalar(.{ .literal = .{ .boolean = insensitive } }),
            try self.scalar(.{ .literal = .{ .boolean = negated } }),
        });
    }

    /// Two grouped hash projections separate existence from NULL evidence.
    /// A plain semi/anti join is insufficient: an unmatched NOT IN must still
    /// become UNKNOWN when the correlated inner set contains SQL NULL.
    fn membership(self: *Builder, expression: *const ast.Scalar) anyerror!*const ast.Scalar {
        if (expression.call.args.len == 1 and expression.call.args[0].* == .call and std.mem.eql(u8, expression.call.args[0].call.name, "$row")) return self.tupleMembership(expression);
        if (try self.quantifiedNeedsApply(expression.call.subquery.?)) return self.quantifiedProducer(expression, .eq, false, false);
        const original = try self.valueQuery(expression.call.subquery.?, true);
        if (expression.call.args.len != 1 or original.columns.len != 1 or original.count_all) return error.InvalidSqlParameters;
        if (original.set_operation != null or original.ctes.len != 0 or original.order_by.len != 0 or original.limit != null or original.offset != null or original.group_by.len != 0 or original.having != null or @import("aggregate_binding.zig").accepts(original.*) or @import("window_binding.zig").accepts(original.*)) return error.UnsupportedSqlShape;
        var local: Names = .empty;
        if (original.source) |source| try aliases(self.alloc, source, &local) else if (original.table) |table| try local.put(self.alloc, table.table, {});
        const value = original.columns[0].expression orelse try self.scalar(.{ .column = original.columns[0].field });
        if (self.referencesOuter(value, local)) return error.UnsupportedSqlShape;
        const operand = try self.rewrite(expression.call.args[0]);
        var keys: std.ArrayList(Key) = .empty;
        const residual = if (original.predicate) |predicate| try self.extract(try self.predicateScalar(predicate), local, &keys, null) else null;
        const zero = try self.scalar(.{ .literal = .{ .integer = 0 } });
        const count = try self.scalar(.{ .call = .{ .name = "count", .args = &.{}, .star = true } });
        var total: *const ast.Scalar = undefined;
        var nonnull: *const ast.Scalar = undefined;
        var matched: *const ast.Scalar = undefined;
        // Both projections are evaluated a bounded number of times, never
        // once per outer row. Native sources share one pinned capture.
        for (0..2) |pass| {
            const membership_pass = pass == 1;
            var query = original.*;
            query.predicate = if (residual) |predicate| blk: {
                const out = try self.alloc.create(ast.Predicate);
                out.* = .{ .scalar = predicate };
                break :blk out;
            } else null;
            const alias = try std.fmt.allocPrint(self.alloc, "$subquery_{d}", .{self.serial});
            self.serial += 1;
            if (self.serial > 64 or self.outer.contains(alias)) return error.SqlProgramLimitExceeded;
            try self.outer.put(self.alloc, alias, {});
            const columns = try self.alloc.alloc(ast.Projection, keys.items.len + 2);
            const groups = try self.alloc.alloc(*const ast.Scalar, keys.items.len + @intFromBool(membership_pass));
            var condition: ?*const ast.Scalar = null;
            for (keys.items, 0..) |key, index| {
                const name = try std.fmt.allocPrint(self.alloc, "$key_{d}", .{index});
                columns[index] = .{ .alias = name, .expression = key.inner };
                groups[index] = try self.groupExpression(key.inner);
                const equal = try self.scalar(.{ .binary = .{ .op = .eq, .left = key.outer, .right = try self.field(alias, name) } });
                condition = if (condition) |prior| try self.scalar(.{ .binary = .{ .op = .@"and", .left = prior, .right = equal } }) else equal;
            }
            columns[keys.items.len] = .{ .alias = "$count", .expression = count };
            if (membership_pass) {
                columns[keys.items.len + 1] = .{ .alias = "$value", .expression = value };
                groups[keys.items.len] = try self.groupExpression(value);
                const equal = try self.scalar(.{ .binary = .{ .op = .eq, .left = operand, .right = try self.field(alias, "$value") } });
                condition = if (condition) |prior| try self.scalar(.{ .binary = .{ .op = .@"and", .left = prior, .right = equal } }) else equal;
                matched = try self.field(alias, "$count");
            } else {
                columns[keys.items.len + 1] = .{ .alias = "$nonnull", .expression = try self.call("count", &.{value}) };
                total = try self.call("coalesce", &.{ try self.field(alias, "$count"), zero });
                nonnull = try self.call("coalesce", &.{ try self.field(alias, "$nonnull"), zero });
            }
            query.columns = columns;
            query.group_by = groups;
            const owned = try self.alloc.create(ast.Select);
            owned.* = query;
            self.source = try self.relation(.{ .join = .{ .kind = .left, .left = self.source, .right = try self.relation(.{ .derived = .{ .query = owned, .alias = alias, .hidden = true } }), .condition = condition } });
        }
        const branches = try self.alloc.alloc(ast.Scalar.Branch, 3);
        branches[0] = .{ .condition = try self.scalar(.{ .binary = .{ .op = .eq, .left = total, .right = zero } }), .value = try self.scalar(.{ .literal = .{ .boolean = false } }) };
        branches[1] = .{ .condition = try self.scalar(.{ .unary = .{ .op = .is_not_null, .operand = matched } }), .value = try self.scalar(.{ .literal = .{ .boolean = true } }) };
        branches[2] = .{ .condition = try self.scalar(.{ .binary = .{ .op = .@"or", .left = try self.scalar(.{ .unary = .{ .op = .is_null, .operand = operand } }), .right = try self.scalar(.{ .binary = .{ .op = .gt, .left = total, .right = nonnull } }) } }), .value = try self.scalar(.{ .cast = .{ .type = .boolean, .operand = try self.scalar(.{ .literal = .null }) } }) };
        return self.scalar(.{ .case_when = .{ .branches = branches, .otherwise = try self.scalar(.{ .literal = .{ .boolean = false } }) } });
    }
    /// Keep keyed summaries for ordinary children, but never move a complete
    /// query boundary or a non-keyed outer predicate into an independent scope.
    fn quantifiedNeedsApply(self: *Builder, query: *const ast.Select) !bool {
        if (scalarBoundary(query.*) or query.count_all or @import("aggregate_binding.zig").accepts(query.*)) {
            // A provably closed boundary still belongs to the shared hash
            // build. Unknown/unqualified references stay in Apply until the
            // catalog binder resolves their lexical ownership.
            const scope: NestedScope = .{ .conservative = true };
            return self.nestedOuterSelect(query, &scope);
        }
        var local: Names = .empty;
        if (query.source) |source| {
            try aliases(self.alloc, source, &local);
            const scope: NestedScope = .{ .source = source };
            if (self.nestedOuterRelation(source, &scope)) return true;
        } else if (query.table) |table| try local.put(self.alloc, table.table, {});
        for (query.columns) |column| {
            if (column.wildcard) return true;
            const value = column.expression orelse try self.scalar(.{ .column = column.field });
            if (self.referencesOuter(value, local)) return true;
            if (query.source == null and query.table == null and value.* == .column) return true;
        }
        if (query.predicate) |predicate| {
            var keys: std.ArrayList(Key) = .empty;
            _ = self.extract(try self.predicateScalar(predicate), local, &keys, null) catch |err| switch (err) {
                error.UnsupportedSqlShape => return true,
                else => return err,
            };
        }
        return false;
    }

    /// Compare actual child outputs after its sort/page/group/window boundary.
    /// Materialize each comparison once; true/false and unknown witnesses have
    /// separate states so empty input retains ANY=false and ALL=true.
    fn quantifiedProducer(self: *Builder, expression: *const ast.Scalar, op: ast.Scalar.Binary, every: bool, negated: bool) anyerror!*const ast.Scalar {
        if (expression.call.args.len != 1) return error.InvalidSqlParameters;
        const operand = try self.prerequisite(try self.rewrite(expression.call.args[0]), null);
        const alias = try self.quantifiedAlias("$quantified_demand");
        const input_alias = try self.quantifiedAlias("$quantified_input");
        const compared_alias = try self.quantifiedAlias("$quantified_compared");
        var comparison = try self.scalar(.{ .binary = .{ .op = op, .left = operand, .right = try self.field(input_alias, "$value") } });
        if (negated) comparison = try self.scalar(.{ .unary = .{ .op = .not, .operand = comparison } });
        const compared = try self.alloc.create(ast.Select);
        compared.* = .{
            .source = try self.relation(.{ .derived = .{ .query = expression.call.subquery.?, .alias = input_alias, .columns = &.{"$value"} } }),
            .columns = try self.alloc.dupe(ast.Projection, &.{.{ .alias = "$comparison", .expression = comparison }}),
        };
        const value = try self.field(compared_alias, "$comparison");
        const decisive = try self.call("bool_or", &.{try self.testValue(if (every) .is_false else .is_true, value)});
        const unknown = try self.call("bool_or", &.{try self.testValue(.is_null, value)});
        const truth = try self.scalar(.{ .case_when = .{
            .branches = try self.alloc.dupe(ast.Scalar.Branch, &.{
                .{ .condition = try self.testValue(.is_true, decisive), .value = try self.scalar(.{ .literal = .{ .boolean = !every } }) },
                .{ .condition = try self.testValue(.is_true, unknown), .value = try self.scalar(.{ .cast = .{ .type = .boolean, .operand = try self.scalar(.{ .literal = .null }) } }) },
            }),
            .otherwise = try self.scalar(.{ .literal = .{ .boolean = every } }),
        } });
        const query = try self.alloc.create(ast.Select);
        query.* = .{
            .source = try self.relation(.{ .derived = .{ .query = compared, .alias = compared_alias } }),
            .columns = try self.alloc.dupe(ast.Projection, &.{.{ .alias = "$value", .expression = truth }}),
        };
        self.source = try self.relation(.{ .join = .{ .kind = .left, .left = self.source, .right = try self.relation(.{ .derived = .{ .query = query, .alias = alias, .hidden = true, .lateral = true } }) } });
        return self.field(alias, "$value");
    }

    fn quantifiedAlias(self: *Builder, prefix: []const u8) ![]const u8 {
        while (self.serial < 64) {
            const name = try std.fmt.allocPrint(self.alloc, "{s}_{d}", .{ prefix, self.serial });
            self.serial += 1;
            // Quoted SQL identifiers can spell our prefixes. Never shadow a
            // target binding merely because it resembles an internal name.
            if (self.outer.contains(name)) continue;
            try self.outer.put(self.alloc, name, {});
            return name;
        }
        return error.SqlProgramLimitExceeded;
    }

    fn tupleMembership(self: *Builder, expression: *const ast.Scalar) anyerror!*const ast.Scalar {
        const original = expression.call.subquery.?;
        const operands = expression.call.args[0].call.args;
        if (operands.len == 0 or operands.len > 256) return error.InvalidSqlSyntax;
        var local: Names = .empty;
        if (original.source) |source| try aliases(self.alloc, source, &local) else if (original.table) |table| try local.put(self.alloc, table.table, {});
        var keys: std.ArrayList(Key) = .empty;
        // Non-keyed correlated query boundaries need an Apply-owned producer;
        // never strip their sort/page/group domain while hoisting a build.
        const residual = if (original.predicate) |predicate| self.extract(try self.predicateScalar(predicate), local, &keys, null) catch |err| switch (err) {
            error.UnsupportedSqlShape => return self.tupleProducer(original, operands),
            else => return err,
        } else null;
        for (original.columns) |column| if (!column.wildcard) {
            const value = column.expression orelse try self.scalar(.{ .column = column.field });
            if (self.referencesOuter(value, local) or (keys.items.len != 0 and value.* != .column and value.* != .literal)) return self.tupleProducer(original, operands);
        };
        if (keys.items.len != 0 and scalarBoundary(original.*)) return self.tupleProducer(original, operands);
        if (keys.items.len + operands.len > 256) return error.SqlProgramLimitExceeded;
        const alias = try std.fmt.allocPrint(self.alloc, "$membership_{d}", .{self.serial});
        self.serial += 1;
        if (self.serial > 64 or self.outer.contains(alias)) return error.SqlProgramLimitExceeded;
        try self.outer.put(self.alloc, alias, {});
        const probes = try self.alloc.alloc(*const ast.Scalar, keys.items.len + operands.len);
        var query = original.*;
        if (keys.items.len != 0) {
            const columns = try self.alloc.alloc(ast.Projection, keys.items.len + original.columns.len);
            for (keys.items, columns[0..keys.items.len], probes[0..keys.items.len]) |key, *column, *probe| {
                column.* = .{ .expression = key.inner };
                probe.* = try self.rewrite(key.outer);
            }
            @memcpy(columns[keys.items.len..], original.columns);
            query.columns = columns;
            query.predicate = if (residual) |value| blk: {
                const predicate = try self.alloc.create(ast.Predicate);
                predicate.* = .{ .scalar = value };
                break :blk predicate;
            } else null;
        }
        for (operands, probes[keys.items.len..]) |operand, *probe| probe.* = try self.rewrite(operand);
        const owned = try self.alloc.create(ast.Select);
        owned.* = query;
        self.source = try self.relation(.{ .join = .{
            .kind = .left,
            .left = self.source,
            .right = try self.relation(.{ .derived = .{ .query = owned, .alias = alias, .hidden = true } }),
            .membership = .{ .probes = probes, .correlations = keys.items.len, .alias = alias },
        } });
        return self.field(alias, "$value");
    }

    /// Preserve complex correlated query boundaries and demand. Summarize the
    /// child's actual output rows, not rows eliminated by its WHERE/sort/page
    /// operators. A row equality is a three-valued conjunction; separate true
    /// and unknown witnesses distinguish an empty/false set from NULL evidence.
    fn tupleProducer(self: *Builder, original: *const ast.Select, operands: []const *const ast.Scalar) anyerror!*const ast.Scalar {
        const alias = try std.fmt.allocPrint(self.alloc, "$tuple_demand_{d}", .{self.serial});
        self.serial += 1;
        if (self.serial > 64 or self.outer.contains(alias)) return error.SqlProgramLimitExceeded;
        try self.outer.put(self.alloc, alias, {});
        const names = try self.alloc.alloc([]const u8, operands.len);
        var comparison: ?*const ast.Scalar = null;
        for (operands, names, 0..) |operand, *name, index| {
            name.* = try std.fmt.allocPrint(self.alloc, "$value_{d}", .{index});
            const value = try self.prerequisite(try self.rewrite(operand), null);
            const equal = try self.scalar(.{ .binary = .{ .op = .eq, .left = value, .right = try self.field("$tuple_input", name.*) } });
            comparison = if (comparison) |prior| try self.scalar(.{ .binary = .{ .op = .@"and", .left = prior, .right = equal } }) else equal;
        }
        const yes = try self.call("bool_or", &.{comparison.?});
        const unknown = try self.call("bool_or", &.{try self.testValue(.is_null, comparison.?)});
        const truth = try self.scalar(.{ .case_when = .{
            .branches = try self.alloc.dupe(ast.Scalar.Branch, &.{
                .{ .condition = try self.testValue(.is_true, yes), .value = try self.scalar(.{ .literal = .{ .boolean = true } }) },
                .{ .condition = try self.testValue(.is_true, unknown), .value = try self.scalar(.{ .cast = .{ .type = .boolean, .operand = try self.scalar(.{ .literal = .null }) } }) },
            }),
            .otherwise = try self.scalar(.{ .literal = .{ .boolean = false } }),
        } });
        const query = try self.alloc.create(ast.Select);
        query.* = .{ .source = try self.relation(.{ .derived = .{ .query = original, .alias = "$tuple_input", .columns = names } }), .columns = try self.alloc.dupe(ast.Projection, &.{.{ .alias = "$value", .expression = truth }}) };
        self.source = try self.relation(.{ .join = .{ .kind = .left, .left = self.source, .right = try self.relation(.{ .derived = .{ .query = query, .alias = alias, .hidden = true, .lateral = true } }) } });
        return self.field(alias, "$value");
    }

    fn rewrite(self: *Builder, input: *const ast.Scalar) anyerror!*const ast.Scalar {
        return self.rewriteDemand(input, null);
    }

    fn mask(self: *Builder, parent: ?*const ast.Scalar, predicate: *const ast.Scalar) !*const ast.Scalar {
        return if (parent) |prior| self.scalar(.{ .binary = .{ .op = .@"and", .left = prior, .right = predicate } }) else predicate;
    }

    fn testValue(self: *Builder, op: ast.Scalar.Unary, value: *const ast.Scalar) !*const ast.Scalar {
        return self.scalar(.{ .unary = .{ .op = op, .operand = value } });
    }

    /// A hidden one-cell producer materializes branch prerequisites exactly
    /// once. It retains the original source's visible names and qualifiers.
    /// Physical children remain in the enclosing captured relation plan.
    fn produce(self: *Builder, value: *const ast.Scalar, demand: ?*const ast.Scalar) !*const ast.Scalar {
        const alias = try std.fmt.allocPrint(self.alloc, "$demand_{d}", .{self.serial});
        self.serial += 1;
        if (self.serial > 64 or self.outer.contains(alias)) return error.SqlProgramLimitExceeded;
        try self.outer.put(self.alloc, alias, {});
        const query = try self.alloc.create(ast.Select);
        query.* = .{ .columns = try self.alloc.dupe(ast.Projection, &.{.{ .alias = "$value", .expression = value }}) };
        self.source = try self.relation(.{ .join = .{
            .kind = .left,
            .left = self.source,
            .right = try self.relation(.{ .derived = .{ .query = query, .alias = alias, .hidden = true, .lateral = true } }),
            .demand = demand,
        } });
        return self.field(alias, "$value");
    }

    fn prerequisite(self: *Builder, value: *const ast.Scalar, demand: ?*const ast.Scalar) !*const ast.Scalar {
        return if (value.* == .literal or value.* == .column) value else self.produce(value, demand);
    }

    /// Preserve the complete child query's domain (including its sort, page,
    /// grouping and HAVING) inside a lateral producer. Summarize only its
    /// resulting rows: the scalar cardinality contract must not count rows
    /// discarded by the child's own operators. Invariant children and hash
    /// builds use the enclosing Apply engine's statement-owned caches.
    fn scalarProducer(self: *Builder, original: *const ast.Select, demand: ?*const ast.Scalar) !*const ast.Scalar {
        // The positional derived-column descriptor validates arity after
        // wildcard/set expansion, even when the producer is never demanded.
        const alias = try std.fmt.allocPrint(self.alloc, "$scalar_demand_{d}", .{self.serial});
        self.serial += 1;
        if (self.serial > 64 or self.outer.contains(alias)) return error.SqlProgramLimitExceeded;
        try self.outer.put(self.alloc, alias, {});
        const bounded = try self.alloc.create(ast.Select);
        bounded.* = original.*;
        bounded.scalar_cardinality_limit = true;
        // An explicit internal bound also disables ordinary unbounded-result
        // overflow sentinels. Parameter limits are validated before capping.
        if (bounded.limit == null) bounded.limit = .{ .integer = 2 };
        const query = try self.alloc.create(ast.Select);
        query.* = .{
            .source = try self.relation(.{ .derived = .{ .query = bounded, .alias = "$scalar_input", .columns = &.{"$value"} } }),
            .columns = try self.alloc.dupe(ast.Projection, &.{
                .{ .alias = "$count", .expression = try self.scalar(.{ .call = .{ .name = "count", .args = &.{}, .star = true } }) },
                .{ .alias = "$value", .expression = try self.call("min", &.{try self.field("$scalar_input", "$value")}) },
            }),
        };
        self.source = try self.relation(.{ .join = .{
            .kind = .left,
            .left = self.source,
            .right = try self.relation(.{ .derived = .{ .query = query, .alias = alias, .hidden = true, .lateral = true } }),
            .demand = demand,
        } });
        return self.call("$single", &.{ try self.field(alias, "$value"), try self.field(alias, "$count") });
    }

    fn rowProducer(self: *Builder, input: *const ast.Scalar, demand: ?*const ast.Scalar) !*const ast.Scalar {
        const original = input.call.subquery.?;
        const width = original.required_output_columns orelse return error.InvalidSqlBackendResponse;
        if (width == 0 or width > 1024 or input.call.args.len != 1) return error.InvalidSqlBackendResponse;
        const argument = input.call.args[0];
        if (argument.* != .literal or argument.literal != .integer) return error.InvalidSqlBackendResponse;
        const slot = std.math.cast(usize, argument.literal.integer) orelse return error.InvalidSqlBackendResponse;
        if (slot >= width) return error.InvalidSqlBackendResponse;
        const output_name = try std.fmt.allocPrint(self.alloc, "$row_{d}", .{slot});
        for (self.row_producers.items) |producer| {
            if (producer.query == original and producer.demand == demand) return self.field(producer.alias, output_name);
        }
        const alias = try std.fmt.allocPrint(self.alloc, "$row_demand_{d}", .{self.serial});
        self.serial += 1;
        if (self.serial > 64 or self.outer.contains(alias)) return error.SqlProgramLimitExceeded;
        try self.outer.put(self.alloc, alias, {});
        const names = try self.alloc.alloc([]const u8, width);
        for (names, 0..) |*name, index| name.* = try std.fmt.allocPrint(self.alloc, "$row_{d}", .{index});
        const bounded = try self.alloc.create(ast.Select);
        bounded.* = original.*;
        bounded.scalar_cardinality_limit = true;
        if (bounded.limit == null) bounded.limit = .{ .integer = 2 };
        self.source = try self.relation(.{ .join = .{
            .kind = .left,
            .left = self.source,
            .right = try self.relation(.{ .derived = .{ .query = bounded, .alias = alias, .columns = names, .hidden = true, .lateral = true } }),
            .demand = demand,
            .single_row = true,
        } });
        try self.row_producers.append(self.alloc, .{ .query = original, .demand = demand, .alias = alias });
        return self.field(alias, output_name);
    }

    fn scalarBoundary(query: ast.Select) bool {
        return query.set_operation != null or query.ctes.len != 0 or query.order_by.len != 0 or query.limit != null or query.offset != null or query.group_by.len != 0 or query.having != null or @import("window_binding.zig").accepts(query);
    }

    fn scalarNeedsApply(self: *Builder, query: ast.Select) !bool {
        if (scalarBoundary(query)) return true;
        if (query.predicate == null and (query.source != null or query.table != null)) return true;
        var local: Names = .empty;
        if (query.source) |source| try aliases(self.alloc, source, &local) else if (query.table) |table| try local.put(self.alloc, table.table, {});
        for (query.columns) |projection| {
            if (projection.wildcard) continue;
            const value = projection.expression orelse try self.scalar(.{ .column = projection.field });
            if (self.referencesOuter(value, local)) return true;
            // Value programs can fail or call a provider. Do not evaluate
            // unused groups/rows through an eager MIN summary before demand.
            if (value.* != .column and value.* != .literal) return true;
            // A source-free child has no local columns. Resolve any names in
            // its actual outer frame, including unqualified references, rather
            // than treating them as inputs to an uncorrelated aggregate.
            if (query.source == null and query.table == null and self.referenceSides(value, local) != 0) return true;
        }
        if (query.predicate) |predicate| {
            // Keep equality decorrelation's grouped, statement-wide fast path.
            // Preflight before modifying the relation: non-keyed correlation
            // instead retains the original predicate inside a lateral child.
            var keys: std.ArrayList(Key) = .empty;
            _ = self.extract(try self.predicateScalar(predicate), local, &keys, null) catch |err| switch (err) {
                error.UnsupportedSqlShape => return true,
                else => return err,
            };
        }
        return false;
    }

    fn rewriteDemand(self: *Builder, input: *const ast.Scalar, demand: ?*const ast.Scalar) anyerror!*const ast.Scalar {
        if (!has(input)) return input;
        if (input.* == .call and input.call.subquery != null) {
            if (std.mem.eql(u8, input.call.name, "$row_scalar")) return self.rowProducer(input, demand);
            if (std.mem.eql(u8, input.call.name, "$scalar") and (demand != null or try self.scalarNeedsApply(input.call.subquery.?.*)))
                return self.scalarProducer(input.call.subquery.?, demand);
            return if (demand != null) self.produce(input, demand) else self.subquery(input);
        }
        return self.scalar(switch (input.*) {
            .call => |part| blk: {
                var copy = part;
                const args = try self.alloc.alloc(*const ast.Scalar, part.args.len);
                var remaining = demand;
                // FILTER belongs to the aggregate's input-row domain. Its
                // argument producers must remain unopened for rejected rows,
                // while the filter itself is evaluated once and remains bound.
                copy.filter = if (part.filter) |filter| try self.prerequisite(try self.rewriteDemand(filter, demand), demand) else null;
                if (copy.filter) |filter| remaining = try self.mask(demand, try self.testValue(.is_true, filter));
                for (part.args, args, 0..) |arg, *out, i| {
                    out.* = try self.rewriteDemand(arg, remaining);
                    if (std.mem.eql(u8, part.name, "coalesce") and i + 1 < args.len) {
                        out.* = try self.prerequisite(out.*, remaining);
                        remaining = try self.mask(remaining, try self.testValue(.is_null, out.*));
                    }
                }
                copy.args = args;
                break :blk .{ .call = copy };
            },
            .unary => |part| .{ .unary = .{ .op = part.op, .operand = try self.rewriteDemand(part.operand, demand) } },
            .binary => |part| blk: {
                var left = try self.rewriteDemand(part.left, demand);
                var right_demand = demand;
                if (part.op == .@"and" or part.op == .@"or") {
                    left = try self.prerequisite(left, demand);
                    right_demand = try self.mask(demand, try self.testValue(if (part.op == .@"and") .is_not_false else .is_not_true, left));
                }
                break :blk .{ .binary = .{ .op = part.op, .left = left, .right = try self.rewriteDemand(part.right, right_demand) } };
            },
            .cast => |part| .{ .cast = .{ .type = part.type, .element_type = part.element_type, .operand = try self.rewriteDemand(part.operand, demand) } },
            .case_when => |part| blk: {
                const branches = try self.alloc.alloc(ast.Scalar.Branch, part.branches.len);
                var remaining = demand;
                for (part.branches, branches) |branch, *out| {
                    const condition = try self.prerequisite(try self.rewriteDemand(branch.condition, remaining), remaining);
                    out.* = .{ .condition = condition, .value = try self.rewriteDemand(branch.value, try self.mask(remaining, try self.testValue(.is_true, condition))) };
                    remaining = try self.mask(remaining, try self.testValue(.is_not_true, condition));
                }
                break :blk .{ .case_when = .{ .branches = branches, .otherwise = if (part.otherwise) |other| try self.rewriteDemand(other, remaining) else null } };
            },
            .in_list => |part| blk: {
                const operand = try self.prerequisite(try self.rewriteDemand(part.operand, demand), demand);
                var remaining: ?*const ast.Scalar = try self.mask(demand, try self.testValue(.is_not_null, operand));
                const values = try self.alloc.alloc(*const ast.Scalar, part.values.len);
                for (part.values, values, 0..) |item, *out, i| {
                    out.* = try self.rewriteDemand(item, remaining);
                    if (i + 1 < values.len) {
                        out.* = try self.prerequisite(out.*, remaining);
                        const equal = try self.scalar(.{ .binary = .{ .op = .eq, .left = operand, .right = out.* } });
                        remaining = try self.mask(remaining, try self.testValue(.is_not_true, equal));
                    }
                }
                break :blk .{ .in_list = .{ .operand = operand, .values = values, .negated = part.negated } };
            },
            else => unreachable,
        });
    }
};
fn rowProjectionReads(statement: ast.Select) bool {
    if (statement.group_by.len != 0 or statement.having != null or
        @import("aggregate_binding.zig").accepts(statement) or @import("window_binding.zig").accepts(statement)) return false;
    for (statement.columns) |column| if (column.expression) |value| if (has(value)) return true;
    return false;
}

fn selectionOrders(alloc: Allocator, statement: ast.Select) !?[]ast.Order {
    if (statement.order_by.len == 0 or !rowProjectionReads(statement)) return null;
    var wildcard = false;
    for (statement.columns) |column| {
        wildcard = wildcard or column.wildcard;
    }
    const normalized = try @import("order_aliases.zig").normalize(alloc, statement);
    const orders = try alloc.dupe(ast.Order, normalized.order_by);
    for (orders) |*order| {
        if (order.position) |position| {
            if (wildcard or position == 0 or position > statement.columns.len) return null;
            const column = statement.columns[position - 1];
            order.position = null;
            order.expression = column.expression orelse blk: {
                const value = try alloc.create(ast.Scalar);
                value.* = .{ .column = column.field };
                break :blk value;
            };
        }
        if (order.expression != null) order.field = "";
    }
    return orders;
}

pub fn lower(alloc: Allocator, statement: ast.Select) !ast.Select {
    if (statement.values_arms.len != 0) {
        const arms = try alloc.alloc(*const ast.Select, statement.values_arms.len);
        for (statement.values_arms, arms) |arm, *out| {
            const rewritten = try alloc.create(ast.Select);
            rewritten.* = if (accepts(arm.*)) try lower(alloc, arm.*) else arm.*;
            out.* = rewritten;
        }
        var result = statement;
        result.values_arms = arms;
        return result;
    }
    if (statement.set_operation) |set| {
        const left = try alloc.create(ast.Select);
        left.* = if (accepts(set.left.*)) try lower(alloc, set.left.*) else set.left.*;
        const right = try alloc.create(ast.Select);
        right.* = if (accepts(set.right.*)) try lower(alloc, set.right.*) else set.right.*;
        var result = statement;
        result.set_operation = .{ .kind = set.kind, .all = set.all, .left = left, .right = right };
        return result;
    }
    if (statement.offset != null and statement.order_by.len == 0 and rowProjectionReads(statement)) {
        // Without a sort PostgreSQL's projection is below OFFSET: a scalar
        // cardinality/value error on a skipped row still belongs to execution.
        // A streaming derived producer preserves that boundary without
        // materializing rows or changing their output labels/types.
        const projected = try alloc.create(ast.Select);
        projected.* = statement;
        projected.limit = null;
        projected.offset = null;
        projected.scalar_cardinality_limit = false;
        const source = try alloc.create(ast.Relation);
        source.* = .{ .derived = .{ .query = projected, .alias = "$offset_output" } };
        return .{
            .source = source,
            .limit = statement.limit,
            .offset = statement.offset,
            .scalar_cardinality_limit = statement.scalar_cardinality_limit,
        };
    }
    var builder: Builder = .{ .alloc = alloc, .source = undefined };
    if (statement.source) |source| builder.source = source else if (statement.table) |table| builder.source = try builder.relation(.{ .table = .{ .name = table } }) else {
        const singleton = try alloc.create(ast.Select);
        singleton.* = .{ .columns = try alloc.dupe(ast.Projection, &.{.{ .expression = try builder.scalar(.{ .literal = .{ .integer = 1 } }) }}) };
        builder.source = try builder.relation(.{ .derived = .{ .query = singleton, .alias = "$singleton", .hidden = true } });
    }
    try aliases(alloc, builder.source, &builder.outer);
    var result = statement;
    // Resolve the row-selection domain before attaching downstream producers.
    // Keep the predicate as a hidden, single-evaluation prerequisite: repeating
    // it in each mask could duplicate expensive or volatile provider work.
    var demand: ?*const ast.Scalar = null;
    var consumers = statement;
    consumers.predicate = null;
    const needs_row_demand = accepts(consumers);
    if (statement.predicate) |predicate| {
        const rewritten = try builder.rewrite(try builder.predicateScalar(predicate));
        const value = if (needs_row_demand) try builder.prerequisite(rewritten, null) else rewritten;
        const out = try alloc.create(ast.Predicate);
        out.* = .{ .scalar = value };
        result.predicate = out;
        if (needs_row_demand) demand = try builder.testValue(.is_true, value);
    }
    const columns = try alloc.dupe(ast.Projection, statement.columns);
    if (try selectionOrders(alloc, statement)) |orders| {
        for (orders) |*order| if (order.expression) |value| {
            if (value.* == .column) continue;
            var matched = false;
            for (statement.columns, columns) |original, *column| {
                if (original.expression != value) continue;
                if (column.expression == value) column.expression = try builder.prerequisite(try builder.rewriteDemand(value, demand), demand);
                if (column.alias == null) column.alias = "?column?";
                order.expression = column.expression;
                matched = true;
                break;
            }
            // Sort-required reads run before selection, independent output
            // reads after it. A shared alias owns one computed value, never
            // a second invocation when the selected row is projected.
            if (!matched and has(value)) order.expression = try builder.prerequisite(try builder.rewriteDemand(value, demand), demand);
        };
        const selected = try alloc.create(ast.Select);
        selected.* = .{
            .source = builder.source,
            .predicate = result.predicate,
            .order_by = orders,
            .order_aliases_expanded = true,
            .limit = statement.limit,
            .offset = statement.offset,
            .scalar_cardinality_limit = statement.scalar_cardinality_limit,
        };
        builder.source = try builder.relation(.{ .derived = .{ .query = selected, .alias = "$selected", .preserve_scope = true } });
        result.predicate = null;
        result.order_by = &.{};
        result.limit = null;
        result.offset = null;
        result.scalar_cardinality_limit = false;
        result.selection_staged = true;
        demand = null;
    }
    for (columns) |*column| if (column.expression) |value| {
        if (column.alias == null and has(value)) column.alias = if (value.* == .call and value.call.subquery != null and std.mem.eql(u8, value.call.name, "$exists")) "exists" else "?column?";
        column.expression = try builder.rewriteDemand(value, demand);
    };
    result.columns = columns;
    const groups = try alloc.alloc(*const ast.Scalar, statement.group_by.len);
    for (statement.group_by, groups) |value, *out| out.* = try builder.rewriteDemand(value, demand);
    result.group_by = groups;
    result.having = if (statement.having) |value| try builder.rewriteDemand(value, demand) else null;
    const orders = try alloc.dupe(ast.Order, result.order_by);
    for (orders) |*order| if (order.expression) |value| {
        order.expression = try builder.rewriteDemand(value, demand);
    };
    result.order_by = orders;
    result.source = builder.source;
    result.table = null;
    return result;
}
