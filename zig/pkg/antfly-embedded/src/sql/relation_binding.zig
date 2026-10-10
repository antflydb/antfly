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

//! Schema-bound logical relations. Physical scans are flattened so execution
//! can pin the complete read set before consuming any row.
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const scalar = @import("scalar.zig");
const describe = @import("describe.zig");
const compiler = @import("compiler.zig");
const Allocator = std.mem.Allocator;

pub const Column = struct {
    name: []const u8,
    internal: []const u8,
    qualifier: []const u8,
    /// Full qualification is available only for unaliased physical tables.
    /// Derived relations and aliases deliberately do not inherit this scope.
    scope: ?catalog.Table.Scope = null,
    type: ast.ColumnType,
    element_type: ?@import("array_value.zig").ElementType = null,
    numeric_modifier: ?scalar.NumericModifier = null,
    nullable: bool,
    visible: bool = true,
    untyped_null: bool = false,
    /// Symbolic lineage exists only during the pre-emission constraint pass.
    origin: ?*const ast.Scalar = null,
    outer_level: u8 = 0,
    outer_frame: ?usize = null,
    outer_ordinal: ?usize = null,
    outer_dependencies: u32 = 0,
    /// Diagnostic-only source domain, never a forwarded raw grouping row.
    grouped_scope: ?*const []const Column = null,
};

/// Expand only authorized visible columns, before allocating expression
/// programs. RETURNING's unqualified * is target-only even for a MERGE join;
/// qualified stars resolve in the caller's already-bound relation domain.
pub fn expandWildcards(alloc: Allocator, columns: []const Column, projections: []const ast.Projection, default_qualifier: ?[]const u8) ![]const ast.Projection {
    const max_output_columns = 1024;
    var count: usize = 0;
    var has_wildcard = false;
    for (projections) |projection| {
        if (!projection.wildcard) {
            count += 1;
            if (count > max_output_columns) return error.SqlProgramLimitExceeded;
            continue;
        }
        has_wildcard = true;
        const before = count;
        for (columns) |column| {
            if (!wildcardMatches(columns, column, projection, default_qualifier)) continue;
            count += 1;
            if (count > max_output_columns) return error.SqlProgramLimitExceeded;
        }
        if (projection.field.len != 0 and count == before) return error.UndefinedColumn;
    }
    if (!has_wildcard) return projections;
    const result = try alloc.alloc(ast.Projection, count);
    var index: usize = 0;
    for (projections) |projection| {
        if (!projection.wildcard) {
            result[index] = projection;
            index += 1;
            continue;
        }
        for (columns, 0..) |column, ordinal| {
            if (!wildcardMatches(columns, column, projection, default_qualifier)) continue;
            result[index] = .{ .bound_column = ordinal, .alias = column.name };
            index += 1;
        }
    }
    return result;
}

fn wildcardMatches(columns: []const Column, column: Column, projection: ast.Projection, default_qualifier: ?[]const u8) bool {
    if (!column.visible) return false;
    const qualifier = if (projection.field.len != 0) projection.field else default_qualifier orelse return column.outer_level == 0;
    for (columns) |other| if (other.outer_level < column.outer_level and qualifierMatches(other, qualifier)) return false;
    return qualifierMatches(column, qualifier);
}

fn qualifierMatches(column: Column, qualifier: []const u8) bool {
    if (std.mem.indexOfScalar(u8, qualifier, 0) == null) return std.mem.eql(u8, column.qualifier, qualifier);
    const scope = column.scope orelse return false;
    return scope.matchesQualifier(qualifier);
}

fn physicalScope(table: catalog.Table, name: ast.Name, aliased: bool) ?catalog.Table.Scope {
    if (aliased) return null;
    return table.scope orelse if (name.namespace) |namespace| .{ .database = name.database orelse "", .namespace = namespace, .name = name.table, .revision = 0 } else null;
}

pub const Node = struct {
    pub const ConstantRef = struct { frame: usize, ordinal: usize };
    columns: []const Column,
    operation: union(enum) {
        singleton,
        recursive_ref: usize,
        outer_ref: usize,
        recursive: struct { id: usize, seed: *const Node, step: *const Node, all: bool },
        materialized_ref: *const Node,
        scan: struct { index: usize, source_columns: []const []const u8 },
        join: struct { kind: ast.JoinKind, left: *const Node, right: *const Node, condition: ?scalar.Program, left_keys: []const scalar.Program, right_keys: []const scalar.Program, correlation: bool = false, membership: ?struct { correlations: usize } = null },
        apply: struct { id: usize, kind: ast.JoinKind, left: *const Node, right: *const Node, condition: ?scalar.Program, demand: ?scalar.Program = null, single_row: bool = false },
        query: struct { source: *const Node, statement: ast.Select, binding: describe.BoundStatement, preserve_scope: bool = false, constant_refs: []const ConstantRef = &.{} },
        set: struct { kind: ast.SetKind, all: bool, left: *const Node, right: *const Node },
        /// Compiler-generated INSERT VALUES arms in input order. Each arm
        /// retains its own captured source dependencies, but execution opens
        /// only one arm iterator at a time after statement capture.
        values: []const *const Node,
        /// Adjacent compiler-owned literal rows share one small operator rather
        /// than each allocating a SELECT program and iterator.
        literal_rows: []const []const scalar.Datum,
        /// Typed prepared mutation images supplied by the execution owner.
        prepared_rows: []const []const u8,
    },
};
pub const virtual_table_name = "$sql_relation";
pub const Bound = struct { root: *const Node, scans: []const catalog.StatementScan, table: catalog.Table, statement: ast.Select, prepared_fields: []const []const u8 = &.{} };
pub const prepared_scope_name = "$sql_prepared_scope";

fn qualified(node: *const ast.Scalar) bool {
    return switch (node.*) {
        .column => |name| std.mem.indexOfScalar(u8, name, 0) != null,
        .literal => false,
        .unary => |part| qualified(part.operand),
        .binary => |part| qualified(part.left) or qualified(part.right),
        .cast => |part| qualified(part.operand),
        .call => |part| blk: {
            if (part.window) |spec| {
                for (spec.partition) |item| if (qualified(item)) break :blk true;
                for (spec.order) |item| {
                    if (std.mem.indexOfScalar(u8, item.field, 0) != null) break :blk true;
                    if (item.expression) |expression_| if (qualified(expression_)) break :blk true;
                }
            }
            for (part.args) |arg| if (qualified(arg)) break :blk true;
            break :blk if (part.filter) |filter| qualified(filter) else false;
        },
        .case_when => |part| blk: {
            for (part.branches) |branch| if (qualified(branch.condition) or qualified(branch.value)) break :blk true;
            break :blk if (part.otherwise) |other| qualified(other) else false;
        },
        .in_list => |part| blk: {
            if (qualified(part.operand)) break :blk true;
            for (part.values) |value| if (qualified(value)) break :blk true;
            break :blk false;
        },
    };
}
fn qualifiedPredicate(node: *const ast.Predicate) bool {
    return switch (node.*) {
        .comparison => |part| std.mem.indexOfScalar(u8, part.field, 0) != null,
        .is_null => |part| std.mem.indexOfScalar(u8, part.field, 0) != null,
        .scalar => |part| qualified(part),
        .negation => |part| qualifiedPredicate(part),
        .conjunction, .disjunction => |part| qualifiedPredicate(part.left) or qualifiedPredicate(part.right),
    };
}
pub fn hasQualifiedScalar(node: *const ast.Scalar) bool {
    return qualified(node);
}
pub fn hasQualifiedPredicate(node: ?*const ast.Predicate) bool {
    return if (node) |predicate| qualifiedPredicate(predicate) else false;
}
pub fn accepts(statement: ast.Select) bool {
    if (statement.source != null or statement.ctes.len != 0 or statement.set_operation != null or statement.values_arms.len != 0) return true;
    for (statement.columns) |projection| {
        if (projection.wildcard) return true;
        if (std.mem.indexOfScalar(u8, projection.field, 0) != null) return true;
        if (projection.expression) |node| if (qualified(node)) return true;
    }
    if (statement.predicate) |node| if (qualifiedPredicate(node)) return true;
    for (statement.group_by) |node| if (qualified(node)) return true;
    if (statement.having) |node| if (qualified(node)) return true;
    for (statement.order_by) |order| {
        if (std.mem.indexOfScalar(u8, order.field, 0) != null) return true;
        if (order.expression) |node| if (qualified(node)) return true;
    }
    return false;
}

pub const ResolveAdapter = struct {
    backend: catalog.Backend,
    table: catalog.Table,
    pub fn iface(self: *ResolveAdapter) catalog.Backend {
        return .{ .ptr = self, .settings_view = self.backend.settings_view, .parameter_fallback_types = self.backend.parameter_fallback_types, .parameter_invocation = self.backend.parameter_invocation, .decision_provider = self.backend.decision_provider, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint } };
    }
    fn resolve(ptr: *anyopaque, _: Allocator, _: ast.Name, action: catalog.Action) !catalog.Table {
        if (action != .read) return error.UnsupportedSqlExecution;
        const self: *ResolveAdapter = @ptrCast(@alignCast(ptr));
        return self.table;
    }
    fn scan(_: *anyopaque, _: Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.UnsupportedSqlExecution;
    }
    fn mutate(_: *anyopaque, _: Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
        return error.UnsupportedSqlExecution;
    }
    fn checkpoint(ptr: *anyopaque) !void {
        const self: *ResolveAdapter = @ptrCast(@alignCast(ptr));
        return self.backend.vtable.checkpoint(self.backend.ptr);
    }
};

/// Joined mutations pin the read/write-authorized target once, then bind all
/// source relations against that same catalog identity. Non-target relations
/// still resolve through the backend with their requested read authority.
pub const TargetResolveAdapter = struct {
    backend: catalog.Backend,
    table: catalog.Table,
    name: ast.Name,
    /// A mutation statement must bind every occurrence of a source against
    /// the same authorized catalog identity, including an optimized rebind.
    cache_sources: bool = false,
    source_tables: std.StringHashMapUnmanaged(catalog.Table) = .empty,
    pub fn iface(self: *@This()) catalog.Backend {
        return .{ .ptr = self, .supports_search_relations = self.backend.supports_search_relations, .settings_view = self.backend.settings_view, .parameter_fallback_types = self.backend.parameter_fallback_types, .parameter_invocation = self.backend.parameter_invocation, .decision_provider = self.backend.decision_provider, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint } };
    }
    fn resolve(ptr: *anyopaque, alloc: Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
        // This adapter is only used while binding the read side of a mutation.
        // Never let a future caller turn the pinned target into an implicit
        // authorization for a second write or an administrative operation.
        if (action != .read) return error.UnsupportedSqlExecution;
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (std.mem.eql(u8, name.table, self.name.table) and std.mem.eql(u8, name.database orelse "", self.name.database orelse "") and std.mem.eql(u8, name.namespace orelse "", self.name.namespace orelse "")) return self.table;
        if (!self.cache_sources) return self.backend.vtable.resolve(self.backend.ptr, alloc, name, action);
        const key = try std.fmt.allocPrint(alloc, "{s}\x00{s}\x00{s}", .{ name.database orelse "", name.namespace orelse "", name.table });
        if (self.source_tables.get(key)) |cached| {
            alloc.free(key);
            return cached;
        }
        errdefer alloc.free(key);
        const resolved = try self.backend.vtable.resolve(self.backend.ptr, alloc, name, action);
        try self.source_tables.put(alloc, key, resolved);
        return resolved;
    }
    fn scan(_: *anyopaque, _: Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.InvalidSqlBackendResponse;
    }
    fn mutate(_: *anyopaque, _: Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
        return error.InvalidSqlBackendResponse;
    }
    fn checkpoint(ptr: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try self.backend.vtable.checkpoint(self.backend.ptr);
    }
};

const PhaseReferences = struct {
    builder: *Builder,
    columns: []const Column,
    needed: *std.StringHashMapUnmanaged(void),
    scope: []const ast.Cte,
    depth: usize,

    pub fn column(self: *@This(), name: []const u8) !void {
        const resolved = try Builder.field(self.columns, name);
        try self.needed.put(self.builder.alloc, resolved.internal, {});
    }
    pub fn subquery(self: *@This(), query: *const ast.Select) !void {
        _ = try self.builder.derivedContext(query, "$phase_probe", &.{}, self.scope, self.depth + 1, true);
    }
};

const PhaseEquality = struct {
    builder: *Builder,
    columns: []const Column,
    bound: std.AutoHashMapUnmanaged(*const ast.Scalar, *const ast.Scalar) = .empty,

    fn expression(self: *@This(), value: *const ast.Scalar) !*const ast.Scalar {
        if (self.bound.get(value)) |prior| return prior;
        const normalized = try self.builder.expression(self.columns, value, &.{});
        try self.bound.put(self.builder.alloc, value, normalized);
        return normalized;
    }
    fn same(ptr: *anyopaque, left: *const ast.Scalar, right: *const ast.Scalar) anyerror!bool {
        if (left == right) return true;
        const aggregates = @import("aggregate_binding.zig");
        const reads = @import("subquery_lowering.zig");
        // Never erase a child's lexical domain to compare scalar syntax.
        if (reads.has(left) or reads.has(right)) return aggregates.same(left, right);
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return aggregates.same(try self.expression(left), try self.expression(right));
    }
};

const Builder = struct {
    alloc: Allocator,
    backend: catalog.Backend,
    parameters: []?ast.ColumnType,
    prepared_scope: ?[]const Column = null,
    prepared_demands: std.StringHashMapUnmanaged(void) = .empty,
    scans: std.ArrayList(catalog.StatementScan) = .empty,
    identities: std.StringHashMapUnmanaged(catalog.Table) = .empty,
    outer_references: ?*std.StringHashMapUnmanaged(void) = null,
    next_column: usize = 0,
    nodes: usize = 0,
    node_limit: usize = 256,
    prepared: std.AutoHashMapUnmanaged(*const ast.Select, Prepared) = .empty,
    shape_only: bool = false,
    shape_expression_nodes: usize = 0,
    shape_columns: std.ArrayList(scalar.Column) = .empty,
    constraints: std.ArrayList(Constraint) = .empty,
    recursive_nodes: std.AutoHashMapUnmanaged(*const ast.Select, *const Node) = .empty,
    materialized_nodes: std.AutoHashMapUnmanaged(*const ast.Select, *const Node) = .empty,
    auto_materialized: std.AutoHashMapUnmanaged(*const ast.Select, void) = .empty,
    recursive_active: ?*const RecursiveFrame = null,
    recursive_next: usize = 0,
    outer_scope: ?*const OuterScope = null,
    outer_next: usize = 0,
    outer_used: [32]bool = @splat(false),
    assignment_expected: []const scalar.Type = &.{},
    const OuterScope = struct { id: usize, columns: []const Column, parent: ?*const OuterScope, cte_boundary: usize };
    const RecursiveFrame = struct { id: usize, query: *const ast.Select, columns: []const Column, parent: ?*const RecursiveFrame };
    const Constraint = struct { expression: *const ast.Scalar, expected: ?ast.ColumnType = null, expected_type: ?scalar.Type = null };

    const Prepared = struct { source: *const Node, lowered: ast.Select, expressions: []const *const ast.Scalar, types: []scalar.Type };

    fn inferenceExpression(self: *Builder, expression_: *const ast.Scalar, columns: []const Column) anyerror!*const ast.Scalar {
        if (expression_.* == .column) for (columns) |column| {
            if (std.mem.eql(u8, column.internal, expression_.column)) if (column.origin) |origin| return origin;
        };
        return self.scalarNode(switch (expression_.*) {
            .column => |name| blk: {
                for (columns) |column| if (column.untyped_null and std.mem.eql(u8, column.internal, name)) break :blk .{ .literal = .null };
                break :blk expression_.*;
            },
            .call => |call| blk: {
                if (call.within_group) |within| {
                    const aggregate_binding = @import("aggregate_binding.zig");
                    const kind = aggregate_binding.orderedKind(call.name) orelse return if (aggregate_binding.aggregateKind(call.name) != null) error.SqlWrongAggregateKind else error.UndefinedSqlFunction;
                    if (call.window != null) return error.UnsupportedSqlShape;
                    if (within.orders.len != 1 or call.args.len != (if (kind == .mode) @as(usize, 1) else 2)) return error.UndefinedSqlFunction;
                    if (call.distinct or call.star) return error.InvalidSqlSyntax;
                    const argument = try self.inferenceExpression(call.args[call.args.len - 1], columns);
                    var result = if (kind == .continuous) try self.scalarNode(.{ .cast = .{ .operand = argument, .type = .number } }) else argument;
                    if (kind != .mode) {
                        const direct = try self.inferenceExpression(call.args[0], columns);
                        const array = try @import("aggregate_binding.zig").arrayExpressionWithInvocation(self.alloc, direct, self.shape_columns.items, self.parameters, self.backend.parameter_invocation);
                        const coerced = try self.scalarNode(.{ .cast = .{ .operand = direct, .type = if (array) .array else .number, .element_type = if (array) .float64 else null } });
                        if (self.shape_only) try self.constraints.append(self.alloc, .{ .expression = coerced });
                        if (array) result = try self.scalarNode(.{ .call = .{ .name = "$array", .args = try self.alloc.dupe(*const ast.Scalar, &.{result}) } });
                    }
                    if (self.shape_only) if (call.filter) |filter| try self.constraints.append(self.alloc, .{ .expression = try self.inferenceExpression(filter, columns), .expected = .boolean });
                    break :blk result.*;
                }
                if (@import("aggregate_binding.zig").orderedKind(call.name)) |kind| return @import("aggregate_binding.zig").orderedWithoutClauseError(kind, call.args.len);
                if (call.window) |spec| {
                    if (self.shape_only) {
                        for (spec.partition) |item| try self.constraints.append(self.alloc, .{ .expression = try self.inferenceExpression(item, columns) });
                        for (spec.order) |item| try self.constraints.append(self.alloc, .{ .expression = try self.inferenceExpression(item.expression orelse try self.scalarNode(.{ .column = item.field }), columns) });
                        if (spec.frame) |frame| for ([_]ast.Window.Bound{ frame.start, frame.end }) |bound| switch (bound) {
                            .preceding, .following => |value| try self.constraints.append(self.alloc, .{ .expression = try self.scalarNode(.{ .literal = value }), .expected = .integer }),
                            else => {},
                        };
                    }
                    const kind = std.meta.stringToEnum(@import("window_binding.zig").Kind, call.name) orelse return error.UndefinedSqlFunction;
                    switch (kind) {
                        .row_number, .rank, .dense_rank => break :blk .{ .literal = .{ .integer = 0 } },
                        .ntile => {
                            if (call.args.len != 1) return error.InvalidSqlParameters;
                            if (self.shape_only) try self.constraints.append(self.alloc, .{ .expression = try self.inferenceExpression(call.args[0], columns), .expected = .integer });
                            break :blk .{ .literal = .{ .integer = 0 } };
                        },
                        .percent_rank, .cume_dist => break :blk .{ .literal = .{ .number = 0 } },
                        .lag, .lead, .first_value, .last_value, .nth_value => {
                            if (call.args.len == 0) return error.InvalidSqlParameters;
                            if (self.shape_only and call.args.len > 1) try self.constraints.append(self.alloc, .{ .expression = try self.inferenceExpression(call.args[1], columns), .expected = .integer });
                            if (call.args.len == 3) {
                                const args = try self.alloc.alloc(*const ast.Scalar, 2);
                                args[0] = try self.inferenceExpression(call.args[0], columns);
                                args[1] = try self.inferenceExpression(call.args[2], columns);
                                break :blk .{ .call = .{ .name = "coalesce", .args = args } };
                            }
                            break :blk (try self.inferenceExpression(call.args[0], columns)).*;
                        },
                        else => {},
                    }
                }
                if (@import("aggregate_binding.zig").aggregateKind(call.name)) |kind| {
                    if (self.shape_only) if (call.filter) |filter| try self.constraints.append(self.alloc, .{ .expression = try self.inferenceExpression(filter, columns), .expected = .boolean });
                    if (kind == .count) break :blk .{ .literal = .{ .integer = 0 } };
                    if (call.args.len != 1) return error.InvalidSqlParameters;
                    const argument = try self.inferenceExpression(call.args[0], columns);
                    break :blk switch (kind) {
                        .avg => .{ .cast = .{ .operand = argument, .type = .number } },
                        .bool_and, .bool_or => .{ .cast = .{ .operand = argument, .type = .boolean } },
                        .pattern_set => .{ .cast = .{ .operand = try self.scalarNode(.{ .cast = .{ .operand = argument, .type = .string } }), .type = .json } },
                        else => argument.*,
                    };
                }
                var copy = call;
                const args = try self.alloc.alloc(*const ast.Scalar, call.args.len);
                for (call.args, args) |arg, *out| out.* = try self.inferenceExpression(arg, columns);
                copy.args = args;
                break :blk .{ .call = copy };
            },
            .binary => |part| .{ .binary = .{ .op = part.op, .left = try self.inferenceExpression(part.left, columns), .right = try self.inferenceExpression(part.right, columns) } },
            .unary => |part| .{ .unary = .{ .op = part.op, .operand = try self.inferenceExpression(part.operand, columns) } },
            .cast => |part| .{ .cast = part.withOperand(try self.inferenceExpression(part.operand, columns)) },
            .case_when => |part| blk: {
                const branches = try self.alloc.alloc(ast.Scalar.Branch, part.branches.len);
                for (part.branches, branches) |branch, *out| out.* = .{ .condition = try self.inferenceExpression(branch.condition, columns), .value = try self.inferenceExpression(branch.value, columns) };
                break :blk .{ .case_when = .{ .branches = branches, .otherwise = if (part.otherwise) |other| try self.inferenceExpression(other, columns) else null } };
            },
            .in_list => |part| blk: {
                const values = try self.alloc.alloc(*const ast.Scalar, part.values.len);
                for (part.values, values) |value, *out| out.* = try self.inferenceExpression(value, columns);
                break :blk .{ .in_list = .{ .operand = try self.inferenceExpression(part.operand, columns), .values = values, .negated = part.negated } };
            },
            else => expression_.*,
        });
    }

    fn constrainSelect(self: *Builder, source: *const Node, query: ast.Select) anyerror![]const *const ast.Scalar {
        const expressions = try self.alloc.alloc(*const ast.Scalar, if (query.count_all) 1 else query.columns.len);
        if (query.count_all) {
            expressions[0] = try self.scalarNode(.{ .cast = .{ .operand = try self.scalarNode(.{ .literal = .null }), .type = .integer } });
        } else for (query.columns, expressions) |projection, *out| {
            out.* = try self.inferenceExpression(projection.expression orelse try self.scalarNode(.{ .column = projection.field }), source.columns);
            try self.constraints.append(self.alloc, .{ .expression = out.* });
        }
        if (query.predicate) |predicate_| {
            const expression_ = try @import("bound_scalars.zig").predicateScalar(self.alloc, try self.virtualTable(source.columns), predicate_);
            try self.constraints.append(self.alloc, .{ .expression = try self.inferenceExpression(expression_, source.columns), .expected = .boolean });
        }
        if (query.having) |having| try self.constraints.append(self.alloc, .{ .expression = try self.inferenceExpression(having, source.columns), .expected = .boolean });
        for (query.order_by) |order| if (order.expression) |expression_| try self.constraints.append(self.alloc, .{ .expression = try self.inferenceExpression(expression_, source.columns) });
        for ([_]?ast.Value{ query.limit, query.offset }) |value| if (value) |bound| try self.constraints.append(self.alloc, .{ .expression = try self.scalarNode(.{ .literal = bound }), .expected = .integer });
        return expressions;
    }

    fn lowerSubqueries(self: *Builder, statement: ast.Select, scope: []const ast.Cte, depth: usize) anyerror!ast.Select {
        const subqueries = @import("subquery_lowering.zig");
        if (!subqueries.accepts(statement)) return statement;
        var prepared = statement;
        const phases = @import("phase_projection.zig");
        if (subqueries.needsOwnProjectionDomain(statement) or phases.accepts(statement)) {
            // Only the source's catalog shape is needed. Keep its authorized
            // identities for executable binding so wildcard ordinals cannot
            // observe a different schema epoch or add physical scan readers.
            var shape: Builder = .{
                .alloc = self.alloc,
                .backend = self.backend,
                .parameters = self.parameters,
                .identities = self.identities,
                .next_column = self.next_column,
                .shape_only = true,
                .prepared_scope = self.prepared_scope,
                .node_limit = self.node_limit,
                .outer_scope = self.outer_scope,
                .outer_next = self.outer_next,
                .recursive_active = self.recursive_active,
                .recursive_next = self.recursive_next,
                .outer_references = self.outer_references,
            };
            try shape.shape_columns.appendSlice(self.alloc, self.shape_columns.items);
            var outer = self.outer_scope;
            while (outer) |frame| : (outer = frame.parent) for (frame.columns) |column| {
                const present = for (shape.shape_columns.items) |known| {
                    if (std.mem.eql(u8, known.name, column.internal)) break true;
                } else false;
                if (!present) try shape.shape_columns.append(self.alloc, .{ .name = column.internal, .type = column.type, .element_type = column.element_type, .numeric_modifier = column.numeric_modifier, .nullable = column.nullable });
            };
            const source = try shape.querySource(statement, scope, depth + 1);
            self.identities = shape.identities;
            const expanded = try expandWildcards(self.alloc, source.columns, statement.columns, null);
            const projections = try self.alloc.dupe(ast.Projection, expanded);
            for (projections) |*projection| if (projection.bound_column) |ordinal| {
                const column = source.columns[ordinal];
                projection.field = if (column.qualifier.len == 0) column.name else try std.fmt.allocPrint(self.alloc, "{s}\x00{s}", .{ column.qualifier, column.name });
                projection.bound_column = null;
            };
            prepared.columns = projections;
            if (phases.accepts(prepared)) {
                try @import("window_binding.zig").validatePlacement(prepared);
                const groups = try self.alloc.dupe(*const ast.Scalar, prepared.group_by);
                for (groups) |*group| {
                    if (group.*.* == .literal and group.*.literal == .integer) {
                        const ordinal = std.math.cast(usize, group.*.literal.integer) orelse return error.SqlGroupingError;
                        if (ordinal == 0 or ordinal > projections.len) return error.SqlGroupingError;
                        const projection = projections[ordinal - 1];
                        group.* = projection.expression orelse try self.scalarNode(.{ .column = projection.field });
                    } else if (group.*.* == .column) {
                        _ = field(source.columns, group.*.column) catch |err| {
                            if (err != error.UndefinedColumn) return err;
                            var found: ?*const ast.Scalar = null;
                            for (projections) |projection| if (std.mem.eql(u8, projection.alias orelse projection.field, group.*.column)) {
                                const value = projection.expression orelse try self.scalarNode(.{ .column = projection.field });
                                if (found) |prior| if (!@import("aggregate_binding.zig").same(prior, value)) return error.AmbiguousSqlColumn;
                                found = value;
                            };
                            group.* = found orelse return err;
                        };
                    }
                }
                prepared.group_by = groups;
                prepared = try @import("order_aliases.zig").normalize(self.alloc, prepared);
                var needed: std.StringHashMapUnmanaged(void) = .empty;
                const child_scope = try self.alloc.alloc(ast.Cte, scope.len + prepared.ctes.len);
                @memcpy(child_scope[0..scope.len], scope);
                @memcpy(child_scope[scope.len..], prepared.ctes);
                if (shape.outer_next == 32) return error.SqlProgramLimitExceeded;
                const frame: OuterScope = .{ .id = shape.outer_next, .columns = source.columns, .parent = self.outer_scope, .cte_boundary = child_scope.len };
                shape.outer_next += 1;
                shape.outer_scope = &frame;
                shape.outer_references = &needed;
                var references: PhaseReferences = .{ .builder = &shape, .columns = source.columns, .needed = &needed, .scope = child_scope, .depth = depth };
                for (prepared.columns) |projection| try phases.visitReferences(projection.expression orelse try self.scalarNode(.{ .column = projection.field }), groups, &references);
                if (prepared.having) |value| try phases.visitReferences(value, groups, &references);
                for (prepared.order_by) |order| if (order.expression) |value| try phases.visitReferences(value, groups, &references);
                self.identities = shape.identities;
                const fields = try self.alloc.alloc(phases.Field, source.columns.len);
                for (source.columns, fields) |column, *out| out.* = .{
                    .name = if (column.qualifier.len == 0) column.name else try std.fmt.allocPrint(self.alloc, "{s}\x00{s}", .{ column.qualifier, column.name }),
                    .qualifier = column.qualifier,
                    .needed = needed.contains(column.internal),
                };
                var equality: PhaseEquality = .{ .builder = &shape, .columns = source.columns };
                prepared = try phases.lower(self.alloc, prepared, fields, .{ .ptr = &equality, .same = PhaseEquality.same });
                return self.lowerSubqueries(prepared, scope, depth + 1);
            }
        }
        const prepared_aliases: ?subqueries.PreparedScope = if (self.prepared_scope) |columns| aliases: {
            const names = try self.alloc.alloc([]const u8, columns.len);
            for (columns, names) |column, *name| name.* = column.qualifier;
            break :aliases .{ .table = prepared_scope_name, .qualifiers = names };
        } else null;
        return subqueries.lowerWithPreparedScope(self.alloc, prepared, prepared_aliases);
    }

    fn inferShape(self: *Builder, statement: ast.Select, expected: []const ast.ColumnType) !void {
        // RETURNING/assignment inference enters directly, before describe's
        // ordinary SELECT normalization. Use the same relational boundary so
        // parameters inside scalar children (including LIMIT) are constrained
        // in their own domain instead of compiling $scalar as a scalar builtin.
        const normalized = try self.lowerSubqueries(statement, &.{}, 0);
        const root = try self.querySource(normalized, &.{}, 0);
        const expressions = try self.constrainSelect(root, try self.lower(root, normalized));
        if (self.assignment_expected.len != 0) {
            if (expressions.len != self.assignment_expected.len) return error.InvalidSqlParameters;
            var contexts: std.AutoHashMapUnmanaged(u32, scalar.Type) = .empty;
            for (expressions, self.assignment_expected) |expression_, descriptor| {
                if (expression_.* == .literal and expression_.literal == .parameter) {
                    const index = expression_.literal.parameter;
                    if (index == 0 or index > self.parameters.len) return error.InvalidSqlParameters;
                    const unknown = if (self.backend.parameter_invocation) |owner| owner.descriptors[index - 1].kind == null else self.parameters[index - 1] == null;
                    if (unknown) {
                        const entry = try contexts.getOrPut(self.alloc, index);
                        if (entry.found_existing and !std.meta.eql(entry.value_ptr.*, descriptor)) return error.ConflictingSqlParameterTypes;
                        entry.value_ptr.* = descriptor;
                    }
                }
                try self.constraints.append(self.alloc, .{ .expression = expression_, .expected_type = descriptor });
            }
        }
        if (expected.len != 0) {
            if (expressions.len != expected.len) return error.InvalidSqlParameters;
            for (expressions, expected) |expression_, kind| try self.constraints.append(self.alloc, .{ .expression = expression_, .expected = kind });
        }
        // A slot changes only from unknown to known. This bounds propagation
        // independently of data cardinality and never evaluates a query.
        for (0..self.parameters.len + 1) |_| {
            try self.backend.vtable.checkpoint(self.backend.ptr);
            var changed = false;
            for (self.constraints.items, 0..) |constraint, index| {
                if (index % 64 == 0) try self.backend.vtable.checkpoint(self.backend.ptr);
                changed = if (constraint.expected_type) |descriptor|
                    try (self.backend.parameter_invocation orelse return error.UnsupportedSqlShape).infer(self.alloc, constraint.expression, self.shape_columns.items, self.parameters, descriptor, .{ .assignment = true }) or changed
                else
                    try scalar.inferParameters(self.alloc, constraint.expression, self.shape_columns.items, self.parameters, constraint.expected, .{ .invocation = self.backend.parameter_invocation }) or changed;
            }
            if (!changed) return;
        }
        return error.SqlProgramLimitExceeded;
    }

    fn collectSet(self: *Builder, query: *const ast.Select, scope: []const ast.Cte, leaves: *std.ArrayList(*const ast.Select), depth: usize) anyerror!void {
        if (depth > 32) return error.SqlProgramLimitExceeded;
        const normalized = try self.lowerSubqueries(query.*, scope, depth);
        const source = try self.querySource(normalized, scope, depth + 1);
        const lowered = try self.lower(source, normalized);
        const expressions = try self.alloc.alloc(*const ast.Scalar, if (lowered.count_all) 1 else lowered.columns.len);
        if (lowered.count_all) {
            expressions[0] = try self.scalarNode(.{ .literal = .{ .integer = 0 } });
        } else for (lowered.columns, expressions) |projection, *out| {
            out.* = try self.inferenceExpression(projection.expression orelse try self.scalarNode(.{ .column = projection.field }), source.columns);
        }
        const types = try self.alloc.alloc(scalar.Type, expressions.len);
        @memset(types, .{});
        try self.prepared.put(self.alloc, query, .{ .source = source, .lowered = lowered, .expressions = expressions, .types = types });
        try leaves.append(self.alloc, query);
    }

    fn inferSet(self: *Builder, query: ast.Select, scope: []const ast.Cte, depth: usize) anyerror!void {
        var leaves: std.ArrayList(*const ast.Select) = .empty;
        const ctes = try self.alloc.alloc(ast.Cte, scope.len + query.ctes.len);
        @memcpy(ctes[0..scope.len], scope);
        @memcpy(ctes[scope.len..], query.ctes);
        // Resolve each binary node before its parent. Coercing every leaf to
        // the root's type would change inner DISTINCT/INTERSECT/EXCEPT keys.
        try self.collectSet(query.set_operation.?.left, ctes, &leaves, depth + 1);
        try self.collectSet(query.set_operation.?.right, ctes, &leaves, depth + 1);
        const width = self.prepared.get(leaves.items[0]).?.expressions.len;
        const common = try self.alloc.alloc(scalar.Type, width);
        for (leaves.items) |leaf| if (self.prepared.get(leaf).?.expressions.len != width) return error.SqlTypeMismatch;
        for (0..self.parameters.len + 2) |_| {
            @memset(common, .{});
            // All arms contribute before any unknown slot is constrained.
            for (leaves.items, 0..) |leaf, leaf_index| {
                const prepared = self.prepared.get(leaf).?;
                const columns = try self.scalarColumns(prepared.source.columns);
                for (prepared.expressions, common) |expression_, *kind| {
                    try mergeInferredType(kind, try self.setType(expression_, columns), leaf_index == 0);
                }
            }
            var changed = false;
            for (leaves.items) |leaf| {
                const prepared = self.prepared.get(leaf).?;
                const columns = try self.scalarColumns(prepared.source.columns);
                if (prepared.lowered.predicate) |predicate_| {
                    const expression_ = try @import("bound_scalars.zig").predicateScalar(self.alloc, try self.virtualTable(prepared.source.columns), predicate_);
                    changed = try scalar.inferParameters(self.alloc, expression_, columns, self.parameters, .boolean, .{ .invocation = self.backend.parameter_invocation }) or changed;
                }
                for (prepared.expressions, common, prepared.types) |expression_, kind, *output| {
                    const resolved = resolveUnknown(kind);
                    changed = try scalar.inferParameters(self.alloc, expression_, columns, self.parameters, resolved.kind, .{ .invocation = self.backend.parameter_invocation }) or changed;
                    output.* = resolved;
                }
            }
            if (!changed) break;
        }
    }

    fn setType(self: *Builder, expression_: *const ast.Scalar, columns: []const scalar.Column) !scalar.Type {
        // A bare SQL string is unknown here; a text cast or a derived text
        // column is concrete. NULL/unknown arms adopt the selected type.
        if (expression_.* == .literal) switch (expression_.literal) {
            .null => return .{},
            .string => return .{ .nullable = false },
            .integer => return .{ .kind = .integer, .nullable = false },
            .number => return .{ .kind = .number, .nullable = false },
            .numeric => return .{ .kind = .number, .element_type = .numeric, .nullable = false },
            .boolean => return .{ .kind = .boolean, .nullable = false },
            .parameter => {},
        };
        return scalar.inferOutputWithInvocation(self.alloc, expression_, columns, self.parameters, self.backend.parameter_invocation);
    }

    fn resolveUnknown(kind: scalar.Type) scalar.Type {
        var resolved = kind;
        if (resolved.kind == null) resolved.kind = .string;
        return resolved;
    }

    fn mergeInferredType(current: *scalar.Type, inferred: scalar.Type, first: bool) !void {
        const modifier = if (first) inferred.numeric_modifier else if (scalar.NumericModifier.eql(current.numeric_modifier, inferred.numeric_modifier)) current.numeric_modifier else null;
        defer current.numeric_modifier = modifier;
        if (inferred.kind == null) return;
        if (current.kind == null) {
            current.* = inferred;
            if (current.element_type == null) {
                if (current.kind == .integer) current.element_type = .int64;
                if (current.kind == .number) current.element_type = .float64;
            }
            return;
        }
        const casts = @import("builtin_cast.zig");
        if (current.kind == .array and inferred.kind == .array) {
            const left = current.element_type orelse return error.InvalidSqlProgram;
            const right = inferred.element_type orelse return error.InvalidSqlProgram;
            current.element_type = if (left == right) left else casts.commonNumeric(left, right) catch return error.SqlCannotCoerce;
        } else if ((current.kind == .integer or current.kind == .number) and (inferred.kind == .integer or inferred.kind == .number)) {
            current.element_type = try casts.commonNumeric(current.element_type orelse (if (current.kind == .integer) .int64 else .float64), inferred.element_type orelse (if (inferred.kind == .integer) .int64 else .float64));
            current.kind = if (casts.integral(current.element_type.?)) .integer else .number;
        } else if (current.kind != inferred.kind) return error.SqlTypeMismatch;
        current.nullable = current.nullable or inferred.nullable;
    }

    fn inferValues(self: *Builder, statement: ast.Select, scope: []const ast.Cte, depth: usize) anyerror![]const scalar.Type {
        const width = statement.values_arms[0].columns.len;
        var leaves: std.ArrayList(*const ast.Select) = .empty;
        for (statement.values_arms, 0..) |arm, index| {
            if (index % 64 == 0) try self.backend.vtable.checkpoint(self.backend.ptr);
            if (arm.columns.len != width) return error.SqlTypeMismatch;
            if (!literalValuesArm(arm)) try self.collectSet(arm, scope, &leaves, depth + 1);
        }
        const common = try self.alloc.alloc(scalar.Type, width);
        for (0..self.parameters.len + 2) |_| {
            @memset(common, .{});
            for (statement.values_arms, 0..) |arm, index| {
                if (index % 64 == 0) try self.backend.vtable.checkpoint(self.backend.ptr);
                if (literalValuesArm(arm)) {
                    for (arm.columns, common) |projection, *kind| {
                        const expression_ = projection.expression orelse return error.InvalidSqlBackendResponse;
                        try mergeInferredType(kind, try self.setType(expression_, &.{}), index == 0);
                    }
                } else {
                    const prepared = self.prepared.get(arm) orelse return error.InvalidSqlBackendResponse;
                    const columns = try self.scalarColumns(prepared.source.columns);
                    for (prepared.expressions, common) |expression_, *kind| try mergeInferredType(kind, try self.setType(expression_, columns), index == 0);
                }
            }
            var changed = false;
            for (leaves.items) |leaf| {
                const prepared = self.prepared.getPtr(leaf) orelse return error.InvalidSqlBackendResponse;
                const columns = try self.scalarColumns(prepared.source.columns);
                if (prepared.lowered.predicate) |predicate_| {
                    const expression_ = try @import("bound_scalars.zig").predicateScalar(self.alloc, try self.virtualTable(prepared.source.columns), predicate_);
                    changed = try scalar.inferParameters(self.alloc, expression_, columns, self.parameters, .boolean, .{ .invocation = self.backend.parameter_invocation }) or changed;
                }
                for (prepared.expressions, common, prepared.types) |expression_, kind, *output| {
                    const resolved = resolveUnknown(kind);
                    changed = try scalar.inferParameters(self.alloc, expression_, columns, self.parameters, resolved.kind, .{ .invocation = self.backend.parameter_invocation }) or changed;
                    output.* = resolved;
                }
            }
            if (!changed) break;
        }
        for (common) |*kind| kind.* = resolveUnknown(kind.*);
        return common;
    }

    fn node(self: *Builder, columns: []const Column, operation: @FieldType(Node, "operation")) !*const Node {
        self.nodes += 1;
        if (self.nodes > self.node_limit) return error.SqlProgramLimitExceeded;
        const result = try self.alloc.create(Node);
        result.* = .{ .columns = columns, .operation = operation };
        return result;
    }
    fn internal(self: *Builder) ![]const u8 {
        defer self.next_column += 1;
        return std.fmt.allocPrint(self.alloc, "$relation_{d}", .{self.next_column});
    }
    fn virtualTable(self: *Builder, columns: []const Column) !catalog.Table {
        const result = try self.alloc.alloc(catalog.Column, columns.len);
        for (columns, result) |column, *out| out.* = .{ .name = column.internal, .path = column.internal, .type = column.type, .element_type = column.element_type, .numeric_modifier = column.numeric_modifier, .nullable = column.nullable };
        return .{ .id = 0, .physical_name = virtual_table_name, .schema_version = 0, .columns = result };
    }
    fn scalarColumns(self: *Builder, columns: []const Column) ![]const scalar.Column {
        const result = try self.alloc.alloc(scalar.Column, columns.len);
        for (columns, result) |column, *out| out.* = .{ .name = column.internal, .type = column.type, .element_type = column.element_type, .numeric_modifier = column.numeric_modifier, .nullable = column.nullable };
        return result;
    }
    fn field(columns: []const Column, name: []const u8) !Column {
        var found: ?Column = null;
        for (columns) |column| {
            // Compiler-owned mutation metadata names themselves begin with
            // NUL. Match the complete bound column suffix before splitting
            // qualification; splitting at the last NUL would lose provenance.
            if (!std.mem.eql(u8, column.name, name)) {
                if (name.len <= column.name.len or !std.mem.endsWith(u8, name, column.name)) continue;
                const separator = name.len - column.name.len - 1;
                if (name[separator] != 0 or !qualifierMatches(column, name[0..separator])) continue;
            }
            if (found) |prior| {
                if (prior.outer_level < column.outer_level) continue;
                if (prior.outer_level == column.outer_level) return error.AmbiguousSqlColumn;
            }
            found = column;
        }
        if (found) |column| return column;
        for (columns) |column| if (column.grouped_scope) |original| {
            const rejected = field(original.*, name) catch |err| {
                if (err == error.UndefinedColumn) continue;
                return err;
            };
            if (rejected.outer_level == 0) return error.SqlGroupingError;
        };
        return error.UndefinedColumn;
    }
    fn resolveField(self: *Builder, columns: []const Column, name: []const u8) !Column {
        const column = try field(columns, name);
        for (0..32) |id| if (column.outer_dependencies & (@as(u32, 1) << @intCast(id)) != 0) {
            self.outer_used[id] = true;
        };
        if (column.outer_frame) |id| {
            self.outer_used[id] = true;
            if (self.outer_references) |references| try references.put(self.alloc, column.internal, {});
        }
        return column;
    }
    fn scalarNode(self: *Builder, value: ast.Scalar) !*const ast.Scalar {
        if (self.shape_only) {
            if (self.shape_expression_nodes >= 32768) return error.SqlProgramLimitExceeded;
            self.shape_expression_nodes += 1;
        }
        const result = try self.alloc.create(ast.Scalar);
        result.* = value;
        return result;
    }
    fn shapePredicate(self: *Builder, columns: []const Column, input: *const ast.Predicate) anyerror!*const ast.Scalar {
        return switch (input.*) {
            .scalar => |expression_| expression_,
            .comparison => |part| blk: {
                var kind: ?ast.ColumnType = null;
                for (columns) |column| if (std.mem.eql(u8, column.internal, part.field)) {
                    kind = if (column.origin) |origin| (try scalar.inferOutputWithInvocation(self.alloc, origin, self.shape_columns.items, self.parameters, self.backend.parameter_invocation)).kind else column.type;
                };
                const literal = try self.scalarNode(.{ .literal = part.value });
                const right = if (kind != null and part.value != .null) try self.scalarNode(.{ .cast = .{ .operand = literal, .type = kind.? } }) else literal;
                break :blk try self.scalarNode(.{ .binary = .{ .op = switch (part.op) {
                    inline else => |tag| @field(ast.Scalar.Binary, @tagName(tag)),
                }, .left = try self.scalarNode(.{ .column = part.field }), .right = right } });
            },
            .is_null => |part| self.scalarNode(.{ .unary = .{ .op = if (part.negated) .is_not_null else .is_null, .operand = try self.scalarNode(.{ .column = part.field }) } }),
            .conjunction, .disjunction => |part| self.scalarNode(.{ .binary = .{ .op = if (input.* == .conjunction) .@"and" else .@"or", .left = try self.shapePredicate(columns, part.left), .right = try self.shapePredicate(columns, part.right) } }),
            .negation => |part| self.scalarNode(.{ .unary = .{ .op = .not, .operand = try self.shapePredicate(columns, part) } }),
        };
    }
    fn expression(self: *Builder, columns: []const Column, input: *const ast.Scalar, aliases: []const ast.Projection) anyerror!*const ast.Scalar {
        return self.scalarNode(switch (input.*) {
            .column => |name| blk: {
                const column = self.resolveField(columns, name) catch |err| fallback: {
                    if (err == error.UndefinedColumn) for (aliases) |projection| if (projection.alias) |alias| if (std.mem.eql(u8, alias, name)) break :fallback Column{ .name = name, .internal = name, .qualifier = "", .type = .string, .nullable = true };
                    return err;
                };
                break :blk if (column.untyped_null) .{ .literal = .null } else .{ .column = column.internal };
            },
            .literal => input.*,
            .unary => |part| .{ .unary = .{ .op = part.op, .operand = try self.expression(columns, part.operand, aliases) } },
            .binary => |part| .{ .binary = .{ .op = part.op, .left = try self.expression(columns, part.left, aliases), .right = try self.expression(columns, part.right, aliases) } },
            .cast => |part| .{ .cast = part.withOperand(try self.expression(columns, part.operand, aliases)) },
            .call => |part| blk: {
                const args = try self.alloc.alloc(*const ast.Scalar, part.args.len);
                for (part.args, args) |arg, *out| out.* = try self.expression(columns, arg, aliases);
                var window = part.window;
                if (window) |*spec| {
                    const partitions = try self.alloc.alloc(*const ast.Scalar, spec.partition.len);
                    for (spec.partition, partitions) |item, *out| out.* = try self.expression(columns, item, &.{});
                    spec.partition = partitions;
                    const order = try self.alloc.dupe(ast.Order, spec.order);
                    for (order) |*item| {
                        if (item.expression) |expression_| {
                            item.expression = try self.expression(columns, expression_, &.{});
                            item.field = "";
                        } else item.field = (try self.resolveField(columns, item.field)).internal;
                    }
                    spec.order = order;
                }
                break :blk .{ .call = .{ .name = part.name, .args = args, .star = part.star, .distinct = part.distinct, .filter = if (part.filter) |filter| try self.expression(columns, filter, aliases) else null, .window = window, .within_group = part.within_group } };
            },
            .case_when => |part| blk: {
                const branches = try self.alloc.alloc(ast.Scalar.Branch, part.branches.len);
                for (part.branches, branches) |branch, *out| out.* = .{ .condition = try self.expression(columns, branch.condition, aliases), .value = try self.expression(columns, branch.value, aliases) };
                break :blk .{ .case_when = .{ .branches = branches, .otherwise = if (part.otherwise) |other| try self.expression(columns, other, aliases) else null } };
            },
            .in_list => |part| blk: {
                const values = try self.alloc.alloc(*const ast.Scalar, part.values.len);
                for (part.values, values) |value, *out| out.* = try self.expression(columns, value, aliases);
                break :blk .{ .in_list = .{ .operand = try self.expression(columns, part.operand, aliases), .values = values, .negated = part.negated } };
            },
        });
    }
    fn predicate(self: *Builder, columns: []const Column, input: *const ast.Predicate) anyerror!*const ast.Predicate {
        const result = try self.alloc.create(ast.Predicate);
        result.* = switch (input.*) {
            .comparison => |part| .{ .comparison = .{ .field = (try self.resolveField(columns, part.field)).internal, .op = part.op, .value = part.value } },
            .is_null => |part| .{ .is_null = .{ .field = (try self.resolveField(columns, part.field)).internal, .negated = part.negated } },
            .scalar => |part| .{ .scalar = try self.expression(columns, part, &.{}) },
            .negation => |part| .{ .negation = try self.predicate(columns, part) },
            .conjunction => |part| .{ .conjunction = .{ .left = try self.predicate(columns, part.left), .right = try self.predicate(columns, part.right) } },
            .disjunction => |part| .{ .disjunction = .{ .left = try self.predicate(columns, part.left), .right = try self.predicate(columns, part.right) } },
        };
        return result;
    }
    fn lower(self: *Builder, source: *const Node, statement: ast.Select) !ast.Select {
        const requested = if (statement.columns.len == 0 and !statement.count_all) &[_]ast.Projection{.{ .wildcard = true }} else statement.columns;
        const expanded = try expandWildcards(self.alloc, source.columns, requested, null);
        var result = statement;
        result.source = null;
        result.ctes = &.{};
        result.set_operation = null;
        result.values_arms = &.{};
        result.table = .{ .table = "$sql_relation" };
        var projections: std.ArrayList(ast.Projection) = .empty;
        for (expanded) |projection| {
            if (projection.expression) |node_| {
                try projections.append(self.alloc, .{ .expression = try self.expression(source.columns, node_, &.{}), .alias = projection.alias });
            } else {
                const column = if (projection.bound_column) |ordinal| source.columns[ordinal] else try self.resolveField(source.columns, projection.field);
                if (column.outer_frame) |id| self.outer_used[id] = true;
                for (0..32) |id| if (column.outer_dependencies & (@as(u32, 1) << @intCast(id)) != 0) {
                    self.outer_used[id] = true;
                };
                if (column.outer_frame != null) if (self.outer_references) |references| try references.put(self.alloc, column.internal, {});
                try projections.append(self.alloc, .{ .field = column.internal, .expression = if (column.untyped_null) try self.scalarNode(.{ .literal = .null }) else null, .alias = projection.alias orelse column.name });
            }
        }
        result.columns = try projections.toOwnedSlice(self.alloc);
        result.predicate = if (statement.predicate) |input| try self.predicate(source.columns, input) else null;
        const groups = try self.alloc.alloc(*const ast.Scalar, statement.group_by.len);
        for (statement.group_by, groups) |input, *out| out.* = try self.expression(source.columns, input, if (input.* == .column) result.columns else &.{});
        result.group_by = groups;
        result.having = if (statement.having) |input| try self.expression(source.columns, input, &.{}) else null;
        const window_orders = @import("window_binding.zig").accepts(statement);
        const order_domain = if (window_orders) try @import("order_aliases.zig").normalize(self.alloc, statement) else statement;
        const orders = try self.alloc.alloc(ast.Order, statement.order_by.len);
        for (order_domain.order_by, orders) |order, *out| {
            out.* = order;
            if (order.expression) |input| {
                out.expression = try self.expression(source.columns, input, &.{});
                // The expression is authoritative. Retaining its original
                // qualified spelling makes semantic binding enter relation
                // lowering again even though all names are already internal.
                out.field = "";
            } else if (order.position == null) {
                var alias = false;
                for (result.columns) |projection| if (projection.alias) |name| if (std.mem.eql(u8, name, order.field)) {
                    alias = true;
                    break;
                };
                if (!alias) out.field = (try self.resolveField(source.columns, order.field)).internal;
            }
        }
        result.order_by = orders;
        result.order_aliases_expanded = statement.order_aliases_expanded or window_orders;
        const windows = try self.alloc.alloc(ast.NamedWindow, statement.windows.len);
        for (statement.windows, windows) |definition, *out| {
            const wrapped = try self.scalarNode(.{ .call = .{ .name = "row_number", .args = &.{}, .window = definition.window } });
            const rewritten = try self.expression(source.columns, wrapped, &.{});
            out.* = .{ .name = definition.name, .window = rewritten.call.window.? };
        }
        result.windows = windows;
        if (result.predicate) |predicate_| {
            const wrapper = try self.alloc.create(ast.Predicate);
            wrapper.* = .{ .scalar = if (self.shape_only) try self.shapePredicate(source.columns, predicate_) else try @import("bound_scalars.zig").predicateScalar(self.alloc, try self.virtualTable(source.columns), predicate_) };
            result.predicate = wrapper;
        }
        if (self.outer_scope != null) {
            for (result.columns) |projection| if (projection.expression) |value| try validateAggregateLevel(self.alloc, source.columns, value);
            if (result.having) |value| try validateAggregateLevel(self.alloc, source.columns, value);
            for (result.order_by) |order| if (order.expression) |value| try validateAggregateLevel(self.alloc, source.columns, value);
        }
        if (result.group_by.len != 0) {
            // Outer-frame values are constants for this lateral invocation,
            // not ungrouped columns of its inner input. Retain only referenced
            // constants in the grouped domain. Do not do this for global
            // aggregates: converting their empty grouping set into keyed
            // grouping would incorrectly remove the empty-input result row.
            var needed: std.StringHashMapUnmanaged(void) = .empty;
            try markSelect(self.alloc, &needed, result);
            var grouped: std.ArrayList(*const ast.Scalar) = .empty;
            try grouped.appendSlice(self.alloc, result.group_by);
            for (source.columns) |column| {
                if (column.outer_frame == null or !needed.contains(column.internal)) continue;
                const present = for (grouped.items) |group| {
                    if (group.* == .column and std.mem.eql(u8, group.column, column.internal)) break true;
                } else false;
                if (!present) try grouped.append(self.alloc, try self.scalarNode(.{ .column = column.internal }));
            }
            result.group_by = grouped.items;
        } else if (@import("aggregate_binding.zig").accepts(result)) {
            // A lateral frame exists even when the inner input is empty.
            // These values belong to the invocation, never to a synthetic
            // grouping key or an arbitrary first/last input row.
            var needed: std.StringHashMapUnmanaged(void) = .empty;
            try markSelect(self.alloc, &needed, result);
            var constants: std.ArrayList([]const u8) = .empty;
            for (source.columns) |column| {
                if (column.outer_frame != null and needed.contains(column.internal))
                    try constants.append(self.alloc, column.internal);
            }
            result.invocation_constants = constants.items;
        }
        return result;
    }

    fn valuesOrigin(self: *Builder, arms: []const *const Node, index: usize, common: scalar.Type) anyerror!*const ast.Scalar {
        if (arms.len == 1) return self.scalarNode(.{ .cast = .{ .operand = arms[0].columns[index].origin orelse return error.InvalidSqlBackendResponse, .type = common.kind.?, .element_type = common.element_type, .numeric_modifier = common.numeric_modifier } });
        const middle = arms.len / 2;
        const arguments = try self.alloc.dupe(*const ast.Scalar, &.{ try self.valuesOrigin(arms[0..middle], index, common), try self.valuesOrigin(arms[middle..], index, common) });
        return self.scalarNode(.{ .call = .{ .name = "coalesce", .args = arguments } });
    }

    fn literalValuesArm(query: *const ast.Select) bool {
        if (!query.generated_values or query.table != null or query.source != null or query.set_operation != null or query.values_arms.len != 0 or query.ctes.len != 0 or query.predicate != null or query.group_by.len != 0 or query.having != null or query.order_by.len != 0 or query.limit != null or query.offset != null) return false;
        for (query.columns) |projection| {
            const expression_ = projection.expression orelse return false;
            if (expression_.* != .literal or expression_.literal == .parameter) return false;
        }
        return true;
    }

    fn literalValuesCompatible(query: *const ast.Select, common: []const scalar.Type) bool {
        if (!literalValuesArm(query)) return false;
        for (query.columns, common) |projection, kind| if (projection.expression.?.literal == .string and kind.kind != .string) return false;
        return true;
    }

    fn literalDatum(a: std.mem.Allocator, expression_: *const ast.Scalar) !scalar.Datum {
        if (expression_.* != .literal) return error.InvalidSqlBackendResponse;
        const value = expression_.literal;
        if (value == .numeric) {
            var budget: @import("array_value.zig").Budget = .{};
            return scalar.numericTextLeaky(a, value.numeric, &budget);
        }
        return .{ .value = switch (value) {
            .integer => |number| .{ .integer = number },
            .number => |number| .{ .float = number },
            .numeric => unreachable,
            .boolean => |boolean| .{ .bool = boolean },
            .string => |string| .{ .string = string },
            .null => .null,
            .parameter => return error.InvalidSqlBackendResponse,
        }, .sql_null = value == .null };
    }

    fn querySource(self: *Builder, statement: ast.Select, scope: []const ast.Cte, depth: usize) anyerror!*const Node {
        const source = try self.querySourceBody(statement, scope, depth);
        if (statement.selection_staged) return source;
        return self.withOuter(source);
    }
    fn withOuter(self: *Builder, source: *const Node) anyerror!*const Node {
        const nearest = self.outer_scope orelse return source;
        var outer: ?*const Node = null;
        var frame: ?*const OuterScope = nearest;
        var level: u8 = 1;
        var forwarded: std.StringHashMapUnmanaged(void) = .empty;
        defer forwarded.deinit(self.alloc);
        for (source.columns) |column| try forwarded.put(self.alloc, column.internal, {});
        while (frame) |current| : ({
            frame = current.parent;
            level += 1;
        }) {
            // A compiler-owned membership probe binds before its enclosing
            // SELECT programs. Do not attach the same lexical frame again
            // when that SELECT subsequently completes source construction.
            var columns: std.ArrayList(Column) = .empty;
            for (current.columns, 0..) |original, ordinal| {
                // A nearer frame can forward a grandparent's identity. Keep
                // that one slot rather than registering its internal name
                // twice in a nested phase/Apply virtual table.
                if (forwarded.contains(original.internal)) continue;
                try forwarded.put(self.alloc, original.internal, {});
                var column = original;
                if (original.outer_frame) |id| column.outer_dependencies |= @as(u32, 1) << @intCast(id);
                column.outer_level = level;
                column.outer_frame = current.id;
                column.outer_ordinal = ordinal;
                try columns.append(self.alloc, column);
            }
            if (columns.items.len == 0) continue;
            const reference = try self.node(try columns.toOwnedSlice(self.alloc), .{ .outer_ref = current.id });
            outer = if (outer) |left| try self.joinNode(left, reference, .cross, null, null) else reference;
        }
        const result = try self.joinNode(outer orelse return source, source, .cross, null, null);
        // Keep the binding domain intact until WHERE has been lowered. Its
        // correlation keys then become a reusable hash build, not a Cartesian
        // scan followed by a residual predicate for every parent.
        if (!self.shape_only) @constCast(result).operation.join.correlation = true;
        return result;
    }

    fn querySourceBody(self: *Builder, statement: ast.Select, scope: []const ast.Cte, depth: usize) anyerror!*const Node {
        if (depth > 32) return error.SqlProgramLimitExceeded;
        // Work backward through the WITH dependency DAG. An inlined later CTE
        // can multiply demand on its producer, while a materialized later CTE
        // evaluates its producer once. Two is enough to choose sharing, so
        // neither reference counts nor planning work grow with execution size.
        const demand = try self.alloc.alloc(u8, statement.ctes.len);
        var body = statement;
        body.ctes = &.{};
        var remaining = statement.ctes.len;
        while (remaining != 0) {
            remaining -= 1;
            const cte = statement.ctes[remaining];
            var count: usize = @min(2, try @import("recursive_shape.zig").references(body, cte.name, 0));
            for (statement.ctes[remaining + 1 ..], demand[remaining + 1 ..]) |later, later_demand| {
                if (later_demand == 0 or count == 2) continue;
                const direct = @min(2, try @import("recursive_shape.zig").references(later.query.*, cte.name, 0));
                const evaluations: usize = if (later.recursive) 2 else if (later.materialization == .materialized or (later.materialization == .automatic and self.auto_materialized.contains(later.query))) 1 else later_demand;
                count = @min(2, count + direct * evaluations);
            }
            demand[remaining] = @intCast(count);
            if (!cte.recursive and cte.materialization == .automatic and count > 1) try self.auto_materialized.put(self.alloc, cte.query, {});
        }
        for (statement.ctes, 0..) |cte, i| if (cte.recursive) {
            for (statement.ctes[i + 1 ..]) |later| if (try @import("recursive_shape.zig").references(cte.query.*, later.name, 0) != 0) return error.UnsupportedSqlShape;
        };
        const ctes = try self.alloc.alloc(ast.Cte, scope.len + statement.ctes.len);
        @memcpy(ctes[0..scope.len], scope);
        @memcpy(ctes[scope.len..], statement.ctes);
        for (statement.ctes, 0..) |cte, index| for (statement.ctes[0..index]) |prior| if (std.mem.eql(u8, cte.name, prior.name)) return error.DuplicateSqlColumn;
        if (statement.values_arms.len != 0) {
            if (!statement.generated_values or statement.set_operation != null or statement.values_arms.len > 1000) return error.InvalidSqlBackendResponse;
            const common = if (self.shape_only) &.{} else try self.inferValues(statement, scope, depth + 1);
            var grouped: std.ArrayList(*const Node) = .empty;
            var arm_index: usize = 0;
            while (arm_index < statement.values_arms.len) {
                if (arm_index % 64 == 0) try self.backend.vtable.checkpoint(self.backend.ptr);
                const leaf = statement.values_arms[arm_index];
                if (!self.shape_only and literalValuesCompatible(leaf, common)) {
                    const first = arm_index;
                    while (arm_index < statement.values_arms.len and literalValuesCompatible(statement.values_arms[arm_index], common)) : (arm_index += 1) {
                        if (arm_index % 64 == 0) try self.backend.vtable.checkpoint(self.backend.ptr);
                    }
                    const columns = try self.alloc.alloc(Column, leaf.columns.len);
                    for (leaf.columns, columns, common) |projection, *column, kind| column.* = .{
                        .name = projection.alias orelse return error.InvalidSqlBackendResponse,
                        .internal = try self.internal(),
                        .qualifier = "",
                        .type = kind.kind orelse .string,
                        .element_type = kind.element_type,
                        .numeric_modifier = kind.numeric_modifier,
                        .nullable = true,
                        .untyped_null = kind.kind == null,
                    };
                    const rows = try self.alloc.alloc([]const scalar.Datum, arm_index - first);
                    for (statement.values_arms[first..arm_index], rows, 0..) |row_query, *row, index| {
                        if (index % 64 == 0) try self.backend.vtable.checkpoint(self.backend.ptr);
                        const values = try self.alloc.alloc(scalar.Datum, row_query.columns.len);
                        for (row_query.columns, values, common) |projection, *value, kind| value.* = try describe.coerceDatum(self.alloc, try literalDatum(self.alloc, projection.expression orelse return error.InvalidSqlBackendResponse), kind.kind.?, kind.element_type);
                        row.* = values;
                    }
                    try grouped.append(self.alloc, try self.node(columns, .{ .literal_rows = rows }));
                } else {
                    if (!self.shape_only and literalValuesArm(leaf)) {
                        var leaves: std.ArrayList(*const ast.Select) = .empty;
                        try self.collectSet(leaf, ctes, &leaves, depth + 1);
                        @memcpy(self.prepared.get(leaf).?.types, common);
                    }
                    try grouped.append(self.alloc, try self.derivedContext(leaf, "", &.{}, ctes, depth + 1, false));
                    arm_index += 1;
                }
            }
            const arms = grouped.items;
            const columns = try self.alloc.dupe(Column, arms[0].columns);
            for (arms[1..]) |arm| {
                if (arm.columns.len != columns.len) return error.SqlTypeMismatch;
                for (columns, arm.columns) |*column, other| {
                    if (self.shape_only) {
                        column.type = .string;
                    } else if (column.type != other.type or column.element_type != other.element_type) return error.SqlTypeMismatch;
                    column.nullable = column.nullable or other.nullable;
                    column.untyped_null = column.untyped_null and other.untyped_null;
                }
            }
            for (columns, 0..) |*column, index| {
                column.internal = try self.internal();
                if (self.shape_only) {
                    // VALUES selects one common type across every row, unlike
                    // binary set operations. Resolve before balancing the
                    // symbolic tree so unknown-only prefixes stay unknown.
                    var inferred: scalar.Type = .{};
                    for (arms, 0..) |arm, merge_arm_index| try mergeInferredType(&inferred, try self.setType(arm.columns[index].origin.?, self.shape_columns.items), merge_arm_index == 0);
                    inferred = resolveUnknown(inferred);
                    column.type = inferred.kind.?;
                    column.element_type = inferred.element_type;
                    column.numeric_modifier = inferred.numeric_modifier;
                    column.origin = try self.valuesOrigin(arms, index, inferred);
                    try self.constraints.append(self.alloc, .{ .expression = column.origin.? });
                }
            }
            return self.node(columns, .{ .values = arms });
        }
        if (statement.set_operation) |set| {
            if (!self.shape_only) try self.inferSet(statement, scope, depth + 1);
            const left = try self.derivedContext(set.left, "", &.{}, ctes, depth + 1, false);
            const right = try self.derivedContext(set.right, "", &.{}, ctes, depth + 1, false);
            if (left.columns.len != right.columns.len) return error.SqlTypeMismatch;
            const columns = try self.alloc.dupe(Column, left.columns);
            for (columns, right.columns) |*column, other| {
                if (self.shape_only) {
                    var common: scalar.Type = .{};
                    try mergeInferredType(&common, try self.setType(column.origin.?, self.shape_columns.items), true);
                    try mergeInferredType(&common, try self.setType(other.origin.?, self.shape_columns.items), false);
                    common = resolveUnknown(common);
                    const args = try self.alloc.dupe(*const ast.Scalar, &.{ try self.scalarNode(.{ .cast = .{ .operand = column.origin.?, .type = common.kind.?, .element_type = common.element_type, .numeric_modifier = common.numeric_modifier } }), try self.scalarNode(.{ .cast = .{ .operand = other.origin.?, .type = common.kind.?, .element_type = common.element_type, .numeric_modifier = common.numeric_modifier } }) });
                    column.origin = try self.scalarNode(.{ .call = .{ .name = "coalesce", .args = args } });
                    column.type = common.kind.?;
                    column.element_type = common.element_type;
                    column.numeric_modifier = common.numeric_modifier;
                    try self.constraints.append(self.alloc, .{ .expression = column.origin.? });
                } else {
                    if (column.type != other.type or column.element_type != other.element_type) return error.SqlTypeMismatch;
                    if (!scalar.NumericModifier.eql(column.numeric_modifier, other.numeric_modifier)) column.numeric_modifier = null;
                }
                column.internal = try self.internal();
                column.nullable = column.nullable or other.nullable;
                column.untyped_null = column.untyped_null and other.untyped_null;
            }
            return self.node(columns, .{ .set = .{ .kind = set.kind, .all = set.all, .left = left, .right = right } });
        }
        if (statement.source) |relation_node| return self.relation(relation_node, ctes, depth + 1);
        if (statement.table) |table| return self.relation(&.{ .table = .{ .name = table } }, ctes, depth + 1);
        return self.node(&.{}, .singleton);
    }
    fn selectionStage(self: *Builder, query: ast.Select, scope: []const ast.Cte, depth: usize) anyerror!*const Node {
        // Bind names once against the original catalog domain, not a derived
        // table alias. Forward the same internal identities and provenance so
        // later correlated producers and wildcard expansion retain their scope.
        const source = try self.querySource(query, scope, depth + 1);
        var boundary = query;
        boundary.columns = &.{.{ .expression = try self.scalarNode(.{ .literal = .{ .integer = 1 } }) }};
        var lowered = try self.lower(source, boundary);
        lowered.internal_projection = true;
        const projections = try self.alloc.alloc(ast.Projection, source.columns.len);
        for (source.columns, projections) |column, *projection| projection.* = .{
            .field = column.internal,
            .alias = column.internal,
            .expression = if (column.untyped_null) try self.scalarNode(.{ .literal = .null }) else null,
        };
        lowered.columns = projections;
        if (self.shape_only) {
            _ = try self.constrainSelect(source, lowered);
            return self.node(source.columns, .singleton);
        }
        const table = try self.virtualTable(source.columns);
        var adapter: ResolveAdapter = .{ .backend = self.backend, .table = table };
        const compiled: compiler.Compiled = .{ .arena = undefined, .statement = .{ .select = lowered }, .parameter_count = @intCast(self.parameters.len) };
        const bound = try describe.bindInternal(self.alloc, adapter.iface(), &compiled, self.parameters);
        for (bound.parameter_types, self.parameters) |hint, *parameter| if (hint != null) {
            parameter.* = hint;
        };
        return self.node(source.columns, .{ .query = .{ .source = source, .statement = lowered, .binding = bound, .preserve_scope = true } });
    }

    fn derived(self: *Builder, query: *const ast.Select, alias: []const u8, names: []const []const u8, scope: []const ast.Cte, depth: usize) anyerror!*const Node {
        return self.derivedContext(query, alias, names, scope, depth, true);
    }

    fn derivedContext(self: *Builder, query: *const ast.Select, alias: []const u8, names: []const []const u8, scope: []const ast.Cte, depth: usize, resolve_unknown: bool) anyerror!*const Node {
        const prepared: ?Prepared = if (self.prepared.fetchRemove(query)) |entry| entry.value else null;
        if (prepared == null and query.set_operation == null and query.values_arms.len == 0 and @import("subquery_lowering.zig").accepts(query.*)) {
            const rewritten = try self.alloc.create(ast.Select);
            rewritten.* = try self.lowerSubqueries(query.*, scope, depth);
            return self.derivedContext(rewritten, alias, names, scope, depth + 1, resolve_unknown);
        }
        var child = if (prepared) |entry| entry.source else try self.querySource(query.*, scope, depth + 1);
        var lowered = if (prepared) |entry| entry.lowered else try self.lower(child, query.*);
        if (self.shape_only) {
            const expressions = try self.constrainSelect(child, lowered);
            if (query.required_output_columns) |width| if (expressions.len != width) return error.InvalidSqlSyntax;
            if (names.len != 0 and names.len != expressions.len) return error.InvalidSqlParameters;
            const columns = try self.alloc.alloc(Column, expressions.len);
            for (columns, expressions, 0..) |*column, expression_, index| {
                const output = try scalar.inferOutputWithInvocation(self.alloc, expression_, self.shape_columns.items, self.parameters, self.backend.parameter_invocation);
                column.* = .{
                    .name = if (names.len != 0) names[index] else if (lowered.count_all) lowered.count_alias orelse "count" else lowered.columns[index].alias orelse if (@import("aggregate_binding.zig").accepts(lowered) and lowered.columns[index].expression != null and lowered.columns[index].expression.?.* == .call) lowered.columns[index].expression.?.call.name else "?column?",
                    .internal = try self.internal(),
                    .qualifier = alias,
                    .type = output.kind orelse .string,
                    .element_type = output.element_type,
                    .numeric_modifier = output.numeric_modifier,
                    .nullable = true,
                    .origin = if (resolve_unknown and (output.kind == null or (expression_.* == .literal and expression_.literal == .string))) try self.scalarNode(.{ .cast = .{ .operand = expression_, .type = .string } }) else expression_,
                };
            }
            return self.node(columns, .singleton);
        }
        if (prepared) |entry| if (!lowered.count_all) {
            const projections = try self.alloc.dupe(ast.Projection, lowered.columns);
            const column_types = try self.scalarColumns(child.columns);
            for (projections, entry.expressions, entry.types) |*projection, expression_, kind| if (kind.kind) |known| {
                const actual = try scalar.inferOutputWithInvocation(self.alloc, expression_, column_types, self.parameters, self.backend.parameter_invocation);
                if (actual.kind != known or actual.element_type != kind.element_type)
                    projection.expression = try self.scalarNode(.{ .cast = .{ .operand = projection.expression orelse try self.scalarNode(.{ .column = projection.field }), .type = known, .element_type = kind.element_type, .numeric_modifier = kind.numeric_modifier } });
            };
            lowered.columns = projections;
        };
        if (child.operation == .join and child.operation.join.correlation and lowered.predicate != null) {
            const join = child.operation.join;
            const expression_ = lowered.predicate.?.scalar;
            child = try self.joinNode(join.left, join.right, .inner, expression_, null);
            lowered.predicate = null;
        }
        const table = try self.virtualTable(child.columns);
        var adapter: ResolveAdapter = .{ .backend = self.backend, .table = table };
        const compiled: compiler.Compiled = .{ .arena = undefined, .statement = .{ .select = lowered }, .parameter_count = @intCast(self.parameters.len) };
        const bound = try describe.bindInternal(self.alloc, adapter.iface(), &compiled, self.parameters);
        for (bound.parameter_types, self.parameters) |hint, *parameter| if (hint != null) {
            parameter.* = hint;
        };
        if (query.required_output_columns) |width| if (bound.columns.len != width) return error.InvalidSqlSyntax;
        if (names.len != 0 and names.len != bound.columns.len) return error.InvalidSqlParameters;
        const columns = try self.alloc.alloc(Column, bound.columns.len);
        for (bound.columns, columns, 0..) |column, *out, index| {
            var untyped = column.untyped_null;
            if (lowered.columns.len > index and lowered.columns[index].expression == null) {
                for (child.columns) |source_column| if (std.mem.eql(u8, source_column.internal, lowered.columns[index].field)) {
                    untyped = untyped or source_column.untyped_null;
                };
            }
            out.* = .{ .name = try self.alloc.dupe(u8, if (names.len == 0) column.name else names[index]), .internal = try self.internal(), .qualifier = alias, .type = column.type, .element_type = column.element_type, .numeric_modifier = column.numeric_modifier, .nullable = true, .untyped_null = !resolve_unknown and untyped };
        }
        const constants = try self.alloc.alloc(Node.ConstantRef, lowered.invocation_constants.len);
        for (lowered.invocation_constants, constants) |name, *reference| {
            reference.* = for (child.columns) |column| {
                if (!std.mem.eql(u8, column.internal, name)) continue;
                break .{ .frame = column.outer_frame orelse return error.InvalidSqlBackendResponse, .ordinal = column.outer_ordinal orelse return error.InvalidSqlBackendResponse };
            } else return error.InvalidSqlBackendResponse;
        }
        return self.node(columns, .{ .query = .{ .source = child, .statement = lowered, .binding = bound, .constant_refs = constants } });
    }
    fn recursiveAlias(self: *Builder, source: *const Node, alias: []const u8) !*const Node {
        const columns = try self.alloc.dupe(Column, source.columns);
        for (columns) |*column| {
            column.internal = try self.internal();
            column.qualifier = alias;
            column.scope = null;
            column.nullable = true;
        }
        return self.node(columns, source.operation);
    }

    fn recursiveCte(self: *Builder, cte: ast.Cte, alias: []const u8, scope: []const ast.Cte, depth: usize) anyerror!*const Node {
        var active = self.recursive_active;
        while (active) |frame| : (active = frame.parent) if (frame.query == cte.query) {
            return self.recursiveAlias(try self.node(frame.columns, .{ .recursive_ref = frame.id }), alias);
        };
        if (self.recursive_nodes.get(cte.query)) |prior| return self.recursiveAlias(prior, alias);
        try @import("recursive_shape.zig").validate(cte);
        if (self.recursive_next >= 32) return error.SqlProgramLimitExceeded;
        const id = self.recursive_next;
        self.recursive_next += 1;
        const set = cte.query.set_operation.?;
        const seed_scope = try self.alloc.alloc(ast.Cte, scope.len + cte.query.ctes.len);
        @memcpy(seed_scope[0..scope.len], scope);
        @memcpy(seed_scope[scope.len..], cte.query.ctes);
        const seed = try self.derived(set.left, cte.name, cte.columns, seed_scope, depth + 1);
        if (seed.columns.len == 0) return error.InvalidSqlParameters;
        const frame: RecursiveFrame = .{ .id = id, .query = cte.query, .columns = seed.columns, .parent = self.recursive_active };
        self.recursive_active = &frame;
        defer self.recursive_active = frame.parent;
        const step_scope = try self.alloc.alloc(ast.Cte, scope.len + 1 + cte.query.ctes.len);
        @memcpy(step_scope[0..scope.len], scope);
        step_scope[scope.len] = cte;
        @memcpy(step_scope[scope.len + 1 ..], cte.query.ctes);
        const step = try self.derived(set.right, cte.name, cte.columns, step_scope, depth + 1);
        if (seed.columns.len != step.columns.len) return error.SqlTypeMismatch;
        const columns = try self.alloc.dupe(Column, seed.columns);
        for (columns, step.columns) |*column, other| {
            column.nullable = true;
            if (self.shape_only) {
                const args = try self.alloc.dupe(*const ast.Scalar, &.{ column.origin.?, other.origin.? });
                column.origin = try self.scalarNode(.{ .call = .{ .name = "coalesce", .args = args } });
                try self.constraints.append(self.alloc, .{ .expression = column.origin.? });
            } else if (!other.untyped_null and column.type != other.type and !(column.type == .number and other.type == .integer)) return error.SqlTypeMismatch;
        }
        const result = try self.node(columns, if (self.shape_only) .singleton else .{ .recursive = .{ .id = id, .seed = seed, .step = step, .all = set.all } });
        try self.recursive_nodes.put(self.alloc, cte.query, result);
        return self.recursiveAlias(result, alias);
    }

    fn relation(self: *Builder, input: *const ast.Relation, scope: []const ast.Cte, depth: usize) anyerror!*const Node {
        if (depth > 32) return error.SqlProgramLimitExceeded;
        try self.backend.vtable.checkpoint(self.backend.ptr);
        return switch (input.*) {
            .table => |reference| blk: {
                if (reference.prepared_rows and std.mem.eql(u8, reference.name.table, prepared_scope_name)) {
                    const authorized = self.prepared_scope orelse return error.InvalidSqlBackendResponse;
                    const columns = try self.alloc.dupe(Column, authorized);
                    const names = try self.alloc.alloc([]const u8, columns.len);
                    for (columns, names) |*column, *name| {
                        name.* = column.internal;
                        column.internal = try self.internal();
                        // The prepared cursor owns typed cells, not expressions
                        // or outer frames from the mutation input's planner.
                        column.origin = null;
                        column.outer_level = 0;
                        column.outer_frame = null;
                        column.outer_ordinal = null;
                        column.outer_dependencies = 0;
                        column.grouped_scope = null;
                        if (self.shape_only) try self.shape_columns.append(self.alloc, .{ .name = column.internal, .type = column.type, .element_type = column.element_type, .numeric_modifier = column.numeric_modifier, .nullable = column.nullable });
                    }
                    break :blk try self.node(columns, .{ .prepared_rows = names });
                }
                if (reference.search == null and !reference.mutation_target and !reference.prepared_rows and reference.name.database == null and reference.name.namespace == null) {
                    var i = scope.len;
                    while (i != 0) {
                        i -= 1;
                        const cte = scope[i];
                        if (std.mem.eql(u8, cte.name, reference.name.table)) {
                            const saved_scope = self.outer_scope;
                            defer self.outer_scope = saved_scope;
                            while (self.outer_scope) |frame| {
                                if (i >= frame.cte_boundary) break;
                                self.outer_scope = frame.parent;
                            }
                            if (cte.recursive and try @import("recursive_shape.zig").references(cte.query.*, cte.name, 0) != 0)
                                break :blk try self.recursiveCte(cte, reference.alias orelse cte.name, scope[0..i], depth + 1);
                            if (cte.materialization == .materialized or (cte.materialization == .automatic and self.auto_materialized.contains(cte.query))) {
                                const materialized = self.materialized_nodes.get(cte.query) orelse source: {
                                    const source = try self.derived(cte.query, cte.name, cte.columns, scope[0..i], depth + 1);
                                    try self.materialized_nodes.put(self.alloc, cte.query, source);
                                    break :source source;
                                };
                                const columns = try self.alloc.dupe(Column, materialized.columns);
                                for (columns) |*column| {
                                    column.internal = try self.internal();
                                    column.qualifier = reference.alias orelse cte.name;
                                }
                                break :blk try self.node(columns, .{ .materialized_ref = materialized });
                            }
                            break :blk try self.derived(cte.query, reference.alias orelse cte.name, cte.columns, scope[0..i], depth + 1);
                        }
                    }
                }
                const identity = try std.fmt.allocPrint(self.alloc, "{s}\x00{s}\x00{s}", .{ reference.name.database orelse "", reference.name.namespace orelse "", reference.name.table });
                const entry = try self.identities.getOrPut(self.alloc, identity);
                if (!entry.found_existing) entry.value_ptr.* = try self.backend.vtable.resolve(self.backend.ptr, self.alloc, reference.name, .read);
                var table = entry.value_ptr.*;
                if (reference.search) |search| {
                    if (!self.backend.supports_search_relations) return error.UnsupportedSqlExecution;
                    if (reference.mutation_target) return error.UnsupportedSqlShape;
                    const extra = [_]catalog.Column{
                        .{ .name = "score", .path = "score", .type = .number },
                        .{ .name = "_highlights", .path = "_highlights", .type = .json },
                    };
                    const augmented = try self.alloc.alloc(catalog.Column, table.columns.len + extra.len);
                    @memcpy(augmented[0..table.columns.len], table.columns);
                    for (extra) |column| for (table.columns) |existing| if (std.mem.eql(u8, column.name, existing.name)) return error.DuplicateSqlColumn;
                    @memcpy(augmented[table.columns.len..], &extra);
                    table.columns = augmented;
                    if (search.request == .parameter) {
                        const slot = search.request.parameter - 1;
                        if (self.parameters[slot]) |kind| {
                            if (kind != .string) return error.ConflictingSqlParameterTypes;
                        }
                        self.parameters[slot] = .string;
                    }
                    if (search.limit) |limit| {
                        if (limit == .parameter) {
                            const slot = limit.parameter - 1;
                            if (self.parameters[slot]) |kind| {
                                if (kind != .integer) return error.ConflictingSqlParameterTypes;
                            }
                            self.parameters[slot] = .integer;
                        } else if (limit != .integer or limit.integer < 1 or limit.integer > 10000) return error.InvalidSqlParameters;
                    }
                }
                if (reference.mutation_presence and !reference.mutation_target) return error.InvalidSqlBackendResponse;
                const metadata_count: usize = if (!reference.mutation_target) 0 else if (reference.mutation_presence) 4 else 3;
                const columns = try self.alloc.alloc(Column, table.columns.len + 1 + metadata_count);
                const source_columns = try self.alloc.alloc([]const u8, columns.len);
                const fields = try self.alloc.alloc([]const u8, table.columns.len);
                for (table.columns, columns[0..table.columns.len], source_columns[0..table.columns.len], fields) |column, *out, *source_name, *field_name| {
                    out.* = .{ .name = try self.alloc.dupe(u8, column.name), .internal = try self.internal(), .qualifier = reference.alias orelse reference.name.table, .scope = physicalScope(table, reference.name, reference.alias != null), .type = column.type, .element_type = column.element_type, .numeric_modifier = column.numeric_modifier, .nullable = column.nullable };
                    source_name.* = column.name;
                    field_name.* = column.path;
                }
                columns[table.columns.len] = .{ .name = "_id", .internal = try self.internal(), .qualifier = reference.alias orelse reference.name.table, .scope = physicalScope(table, reference.name, reference.alias != null), .type = .string, .nullable = false, .visible = reference.search != null };
                for (@import("joined_mutation.zig").metadata_fields[0..metadata_count], 0..) |name, i| {
                    columns[table.columns.len + 1 + i] = .{ .name = name, .internal = try self.internal(), .qualifier = reference.alias orelse reference.name.table, .type = if (i == 2) .json else .string, .nullable = i == 2, .visible = false };
                    source_columns[table.columns.len + 1 + i] = name;
                }
                if (self.shape_only) for (columns) |column| try self.shape_columns.append(self.alloc, .{ .name = column.internal, .type = column.type, .element_type = column.element_type, .numeric_modifier = column.numeric_modifier, .nullable = column.nullable });
                source_columns[table.columns.len] = "_id";
                if (reference.prepared_rows) {
                    if (reference.mutation_target or reference.mutation_document or reference.mutation_presence) return error.InvalidSqlBackendResponse;
                    break :blk try self.node(columns, .{ .prepared_rows = source_columns });
                }
                const index = self.scans.items.len;
                if (index >= 64) return error.SqlProgramLimitExceeded;
                const search_request = if (reference.search) |search| search_request: {
                    const request = try self.alloc.create(catalog.Scan.Search);
                    request.* = .{ .expression = search.* };
                    break :search_request request;
                } else null;
                try self.scans.append(self.alloc, .{ .table = table, .request = .{ .search = search_request, .fields = fields, .limit = 256, .include_primary_digest = reference.mutation_target, .include_document = reference.mutation_document and table.storage_mode == .document } });
                break :blk try self.node(columns, .{ .scan = .{ .index = index, .source_columns = source_columns } });
            },
            .derived => |query| blk: {
                const result = if (query.preserve_scope) try self.selectionStage(query.query.*, scope, depth + 1) else try self.derived(query.query, query.alias, query.columns, scope, depth + 1);
                if (query.phase_scope) |phase| {
                    if (phase.columns.len != result.columns.len) return error.InvalidSqlBackendResponse;
                    const original = if (self.shape_only) try self.querySource(query.query.*, scope, depth + 1) else if (result.operation == .query) result.operation.query.source else return error.InvalidSqlBackendResponse;
                    const grouped_scope = if (phase.grouped) try self.alloc.create([]const Column) else null;
                    if (grouped_scope) |domain| domain.* = original.columns;
                    for (@constCast(result.columns), phase.columns) |*column, reference| {
                        if (reference) |name| {
                            const source_column = try field(original.columns, name);
                            column.name = source_column.name;
                            column.qualifier = source_column.qualifier;
                            column.scope = source_column.scope;
                            column.visible = source_column.visible;
                        } else column.visible = false;
                    }
                    if (result.columns.len != 0) @constCast(result.columns)[0].grouped_scope = grouped_scope;
                }
                if (query.hidden) for (@constCast(result.columns)) |*column| {
                    column.visible = false;
                };
                break :blk result;
            },
            .join => |join| blk: {
                const left = try self.relation(join.left, scope, depth + 1);
                const lateral = join.right.* == .derived and join.right.derived.lateral;
                var apply_id: ?usize = null;
                const right = if (lateral) right: {
                    if (self.outer_next == 32) return error.SqlProgramLimitExceeded;
                    const id = self.outer_next;
                    self.outer_next += 1;
                    const frame: OuterScope = .{ .id = id, .columns = left.columns, .parent = self.outer_scope, .cte_boundary = scope.len };
                    const saved_scope = self.outer_scope;
                    defer self.outer_scope = saved_scope;
                    self.outer_scope = &frame;
                    const source = try self.relation(join.right, scope, depth + 1);
                    if (join.kind == .right or join.kind == .full) {
                        if (self.outer_used[id]) return error.InvalidLateralReference;
                        try self.uncorrelated(source, id);
                    } else {
                        apply_id = id;
                        // An unused outer binding must not prevent replay of
                        // an invariant producer after its first demanded row.
                        if (!self.outer_used[id]) try self.uncorrelated(source, id);
                    }
                    break :right source;
                } else try self.relation(join.right, scope, depth + 1);
                if (join.membership) |membership| {
                    if (join.kind != .left or join.condition != null or join.demand != null or lateral) return error.InvalidSqlBackendResponse;
                    break :blk try self.membershipNode(left, right, membership.probes, membership.correlations, membership.alias);
                }
                for (left.columns) |a| for (right.columns) |b| if (std.mem.eql(u8, a.qualifier, b.qualifier)) return error.AmbiguousSqlColumn;
                const columns = try self.joinColumns(left, right, join.kind);
                const expression_ = if (join.condition) |condition| try self.expression(columns, condition, &.{}) else null;
                var demand: ?scalar.Program = null;
                if (join.demand) |mask| {
                    if (!lateral or join.kind != .left or apply_id == null) return error.InvalidSqlBackendResponse;
                    const guard_expression = try self.expression(left.columns, mask, &.{});
                    if (self.shape_only) {
                        try self.constraints.append(self.alloc, .{ .expression = try self.inferenceExpression(guard_expression, left.columns), .expected = .boolean });
                    } else {
                        const types = try self.scalarColumns(left.columns);
                        _ = try scalar.inferParameters(self.alloc, guard_expression, types, self.parameters, .boolean, .{ .invocation = self.backend.parameter_invocation });
                        demand = try scalar.bindExpectedWithSettings(self.alloc, guard_expression, types, self.parameters, .boolean, .{ .invocation = self.backend.parameter_invocation }, self.backend.settings_view);
                    }
                }
                const result = try self.joinNode(left, right, join.kind, expression_, apply_id);
                if (join.single_row) {
                    if (!lateral or join.kind != .left or apply_id == null or join.condition != null or join.membership != null) return error.InvalidSqlBackendResponse;
                    if (!self.shape_only) @constCast(result).operation.apply.single_row = true;
                }
                if (!self.shape_only and demand != null) @constCast(result).operation.apply.demand = demand;
                break :blk result;
            },
        };
    }
    fn joinColumns(self: *Builder, left: *const Node, right: *const Node, kind: ast.JoinKind) ![]Column {
        const columns = try self.alloc.alloc(Column, left.columns.len + right.columns.len);
        @memcpy(columns[0..left.columns.len], left.columns);
        @memcpy(columns[left.columns.len..], right.columns);
        if (kind == .right or kind == .full) for (columns[0..left.columns.len]) |*column| {
            column.nullable = true;
        };
        if (kind == .left or kind == .full) for (columns[left.columns.len..]) |*column| {
            column.nullable = true;
        };
        return columns;
    }

    fn membershipNode(self: *Builder, input: *const Node, right: *const Node, probes: []const *const ast.Scalar, correlations: usize, alias: []const u8) !*const Node {
        const left = try self.withOuter(input);
        if (probes.len != right.columns.len or probes.len <= correlations or probes.len > 256) return error.InvalidSqlSyntax;
        const columns = try self.alloc.alloc(Column, left.columns.len + 1);
        @memcpy(columns[0..left.columns.len], left.columns);
        columns[left.columns.len] = .{ .name = "$value", .internal = try self.internal(), .qualifier = alias, .type = .boolean, .nullable = true, .visible = false };
        const left_programs = try self.alloc.alloc(scalar.Program, if (self.shape_only) 0 else probes.len);
        const right_programs = try self.alloc.alloc(scalar.Program, if (self.shape_only) 0 else probes.len);
        const left_types = try self.scalarColumns(left.columns);
        const right_types = try self.scalarColumns(right.columns);
        for (probes, right.columns, 0..) |probe, right_column, index| {
            const left_expression = try self.expression(left.columns, probe, &.{});
            const right_expression = try self.scalarNode(.{ .column = right_column.internal });
            if (self.shape_only) {
                const comparison = try self.scalarNode(.{ .binary = .{ .op = .eq, .left = try self.inferenceExpression(left_expression, left.columns), .right = right_column.origin orelse return error.InvalidSqlBackendResponse } });
                try self.constraints.append(self.alloc, .{ .expression = comparison, .expected = .boolean });
                continue;
            }
            var common = try self.setType(left_expression, left_types);
            // Equality's polymorphic anyarray signature requires one exact
            // element type. Unlike UNION/VALUES, it does not promote int2[]
            // to int8[] or float[] merely because their cells are numeric.
            if (common.kind == .array and right_column.type == .array and !right_column.untyped_null and common.element_type != right_column.element_type) return error.SqlUndefinedOperator;
            mergeInferredType(&common, if (right_column.untyped_null) .{} else .{ .kind = right_column.type, .element_type = right_column.element_type, .numeric_modifier = right_column.numeric_modifier, .nullable = right_column.nullable }, false) catch |err| return switch (err) {
                error.SqlTypeMismatch, error.SqlCannotCoerce => error.SqlUndefinedOperator,
                else => err,
            };
            common = resolveUnknown(common);
            const l = try self.scalarNode(.{ .cast = .{ .operand = left_expression, .type = common.kind.?, .element_type = common.element_type, .numeric_modifier = common.numeric_modifier } });
            const r = try self.scalarNode(.{ .cast = .{ .operand = right_expression, .type = common.kind.?, .element_type = common.element_type, .numeric_modifier = common.numeric_modifier } });
            left_programs[index] = try scalar.bindWithSettings(self.alloc, l, left_types, self.parameters, .{ .invocation = self.backend.parameter_invocation }, self.backend.settings_view);
            right_programs[index] = try scalar.bindWithSettings(self.alloc, r, right_types, self.parameters, .{ .invocation = self.backend.parameter_invocation }, self.backend.settings_view);
        }
        if (self.shape_only) columns[left.columns.len].origin = try self.scalarNode(.{ .cast = .{ .type = .boolean, .operand = try self.scalarNode(.{ .literal = .null }) } });
        return self.node(columns, if (self.shape_only) .singleton else .{ .join = .{ .kind = .left, .left = left, .right = right, .condition = null, .left_keys = left_programs, .right_keys = right_programs, .membership = .{ .correlations = correlations } } });
    }

    fn joinNode(self: *Builder, left: *const Node, right: *const Node, kind: ast.JoinKind, expression_: ?*const ast.Scalar, apply_id: ?usize) !*const Node {
        const columns = try self.joinColumns(left, right, kind);
        if (self.shape_only) {
            if (expression_) |condition| try self.constraints.append(self.alloc, .{ .expression = try self.inferenceExpression(condition, columns), .expected = .boolean });
            return self.node(columns, .singleton);
        }
        const column_types = try self.scalarColumns(columns);
        if (expression_) |condition| _ = try scalar.inferParameters(self.alloc, condition, column_types, self.parameters, .boolean, .{ .invocation = self.backend.parameter_invocation });
        const program = if (expression_) |condition| try scalar.bindExpectedWithSettings(self.alloc, condition, column_types, self.parameters, .boolean, .{ .invocation = self.backend.parameter_invocation }, self.backend.settings_view) else null;
        if (program) |bound| if (bound.output_type.kind != null and bound.output_type.kind != .boolean) return error.SqlTypeMismatch;
        if (apply_id) |id| return self.node(columns, .{ .apply = .{ .id = id, .kind = kind, .left = left, .right = right, .condition = program } });
        var left_keys: std.ArrayList(scalar.Program) = .empty;
        var right_keys: std.ArrayList(scalar.Program) = .empty;
        if (expression_) |condition| try self.joinKeys(condition, left, right, &left_keys, &right_keys);
        return self.node(columns, .{ .join = .{ .kind = kind, .left = left, .right = right, .condition = program, .left_keys = try left_keys.toOwnedSlice(self.alloc), .right_keys = try right_keys.toOwnedSlice(self.alloc) } });
    }

    fn uncorrelated(self: *Builder, source: *const Node, id: usize) anyerror!void {
        switch (source.operation) {
            .outer_ref => |reference| if (reference == id) {
                const nulls = try self.alloc.alloc(scalar.Datum, source.columns.len);
                @memset(nulls, .{});
                @constCast(source).operation = .{ .literal_rows = try self.alloc.dupe([]const scalar.Datum, &.{nulls}) };
            },
            .query => |query| try self.uncorrelated(query.source, id),
            .materialized_ref => |node_| try self.uncorrelated(node_, id),
            .join => |join| {
                try self.uncorrelated(join.left, id);
                try self.uncorrelated(join.right, id);
            },
            .apply => |apply| {
                try self.uncorrelated(apply.left, id);
                try self.uncorrelated(apply.right, id);
            },
            .set => |set| {
                try self.uncorrelated(set.left, id);
                try self.uncorrelated(set.right, id);
            },
            .recursive => |part| {
                try self.uncorrelated(part.seed, id);
                try self.uncorrelated(part.step, id);
            },
            .values => |arms| for (arms) |arm| try self.uncorrelated(arm, id),
            else => {},
        }
    }
    fn joinKeys(self: *Builder, input: *const ast.Scalar, left: *const Node, right: *const Node, left_keys: *std.ArrayList(scalar.Program), right_keys: *std.ArrayList(scalar.Program)) anyerror!void {
        if (input.* != .binary) return;
        const binary = input.binary;
        if (binary.op == .@"and") {
            try self.joinKeys(binary.left, left, right, left_keys, right_keys);
            try self.joinKeys(binary.right, left, right, left_keys, right_keys);
            return;
        }
        if (binary.op != .eq) return;
        var left_node = binary.left;
        var right_node = binary.right;
        if (!sideLocal(left.columns, left_node) or !sideLocal(right.columns, right_node)) std.mem.swap(*const ast.Scalar, &left_node, &right_node);
        if (!sideLocal(left.columns, left_node) or !sideLocal(right.columns, right_node)) return;
        try left_keys.append(self.alloc, try scalar.bindWithSettings(self.alloc, left_node, try self.scalarColumns(left.columns), self.parameters, .{ .invocation = self.backend.parameter_invocation }, self.backend.settings_view));
        try right_keys.append(self.alloc, try scalar.bindWithSettings(self.alloc, right_node, try self.scalarColumns(right.columns), self.parameters, .{ .invocation = self.backend.parameter_invocation }, self.backend.settings_view));
    }
};
/// Expressions whose inputs belong to one side are valid hash keys too.
/// Restricting extraction to bare columns makes computed IN/join operands
/// unexpectedly quadratic despite a perfectly usable equality key.
fn sideLocal(columns: []const Column, input: *const ast.Scalar) bool {
    return switch (input.*) {
        .literal => true,
        .column => |name| hasInternal(columns, name),
        .unary => |part| sideLocal(columns, part.operand),
        .cast => |part| sideLocal(columns, part.operand),
        .binary => |part| sideLocal(columns, part.left) and sideLocal(columns, part.right),
        .call => |part| blk: {
            if (part.subquery != null or part.window != null or part.star or part.distinct or part.filter != null) break :blk false;
            for (part.args) |arg| if (!sideLocal(columns, arg)) break :blk false;
            break :blk true;
        },
        .case_when => |part| blk: {
            for (part.branches) |branch| if (!sideLocal(columns, branch.condition) or !sideLocal(columns, branch.value)) break :blk false;
            break :blk if (part.otherwise) |other| sideLocal(columns, other) else true;
        },
        .in_list => |part| blk: {
            if (!sideLocal(columns, part.operand)) break :blk false;
            for (part.values) |value| if (!sideLocal(columns, value)) break :blk false;
            break :blk true;
        },
    };
}
fn hasInternal(columns: []const Column, name: []const u8) bool {
    for (columns) |column| if (std.mem.eql(u8, column.internal, name)) return true;
    return false;
}

pub fn outputUntypedNull(bound: Bound, index: usize) bool {
    if (index >= bound.statement.columns.len) return false;
    const projection = bound.statement.columns[index];
    if (projection.expression != null) return false;
    for (bound.root.columns) |column| if (std.mem.eql(u8, column.internal, projection.field)) return column.untyped_null;
    return false;
}

fn markExpression(alloc: Allocator, needed: *std.StringHashMapUnmanaged(void), input: *const ast.Scalar) anyerror!void {
    switch (input.*) {
        .column => |name| try needed.put(alloc, name, {}),
        .literal => {},
        .unary => |part| try markExpression(alloc, needed, part.operand),
        .binary => |part| {
            try markExpression(alloc, needed, part.left);
            try markExpression(alloc, needed, part.right);
        },
        .cast => |part| try markExpression(alloc, needed, part.operand),
        .call => |part| {
            // Binding already validated the discarded EXISTS expression.
            // It has no runtime dependencies and must not fetch cold payloads.
            if (std.mem.eql(u8, part.name, "$validate")) return;
            for (part.args) |arg| try markExpression(alloc, needed, arg);
            if (part.filter) |filter| try markExpression(alloc, needed, filter);
            if (part.window) |spec| {
                for (spec.partition) |item| try markExpression(alloc, needed, item);
                for (spec.order) |item| if (item.expression) |expression_| try markExpression(alloc, needed, expression_) else try needed.put(alloc, item.field, {});
            }
        },
        .case_when => |part| {
            for (part.branches) |branch| {
                try markExpression(alloc, needed, branch.condition);
                try markExpression(alloc, needed, branch.value);
            }
            if (part.otherwise) |other| try markExpression(alloc, needed, other);
        },
        .in_list => |part| {
            try markExpression(alloc, needed, part.operand);
            for (part.values) |value| try markExpression(alloc, needed, value);
        },
    }
}
fn markPredicate(alloc: Allocator, needed: *std.StringHashMapUnmanaged(void), input: *const ast.Predicate) anyerror!void {
    switch (input.*) {
        .comparison => |part| try needed.put(alloc, part.field, {}),
        .is_null => |part| try needed.put(alloc, part.field, {}),
        .scalar => |part| try markExpression(alloc, needed, part),
        .negation => |part| try markPredicate(alloc, needed, part),
        .conjunction, .disjunction => |part| {
            try markPredicate(alloc, needed, part.left);
            try markPredicate(alloc, needed, part.right);
        },
    }
}
fn markSelect(alloc: Allocator, needed: *std.StringHashMapUnmanaged(void), statement: ast.Select) !void {
    for (statement.columns) |projection| if (projection.expression) |expression| {
        try markExpression(alloc, needed, expression);
    } else {
        try needed.put(alloc, projection.field, {});
    };
    if (statement.predicate) |predicate| try markPredicate(alloc, needed, predicate);
    for (statement.group_by) |expression| try markExpression(alloc, needed, expression);
    if (statement.having) |expression| try markExpression(alloc, needed, expression);
    for (statement.order_by) |order| if (order.expression) |expression| {
        try markExpression(alloc, needed, expression);
    } else if (order.position == null) {
        try needed.put(alloc, order.field, {});
    };
}
fn projectScans(builder: *Builder, node: *const Node, needed: *std.StringHashMapUnmanaged(void)) anyerror!void {
    switch (node.operation) {
        .singleton, .recursive_ref, .outer_ref, .literal_rows => {},
        .prepared_rows => |names| {
            if (builder.prepared_scope != null) for (node.columns, names) |column, name| {
                if (needed.contains(column.internal)) try builder.prepared_demands.put(builder.alloc, name, {});
            };
        },
        .materialized_ref => |source| {
            // A materialized CTE stores its complete declared output once;
            // references can project different columns without changing its
            // producer's physical scan contract.
            for (source.columns) |column| try needed.put(builder.alloc, column.internal, {});
            try projectScans(builder, source, needed);
        },
        .recursive => |recursive| {
            // The working row is positional. Every recursive output is needed
            // to seed the next iteration even when the outer SELECT projects less.
            try projectScans(builder, recursive.seed, needed);
            try projectScans(builder, recursive.step, needed);
        },
        .scan => |scan| {
            var fields: std.ArrayList([]const u8) = .empty;
            const request = &builder.scans.items[scan.index];
            for (node.columns, scan.source_columns) |column, name| {
                if (!needed.contains(column.internal) or std.mem.eql(u8, name, "_id") or @import("joined_mutation.zig").isMetadata(name)) continue;
                try fields.append(builder.alloc, (try request.table.column(name)).path);
            }
            request.request.fields = try fields.toOwnedSlice(builder.alloc);
        },
        .join => |join| {
            if (join.condition) |program| for (program.required_columns) |ordinal| try needed.put(builder.alloc, node.columns[ordinal].internal, {});
            for (join.left_keys) |program| for (program.required_columns) |ordinal| try needed.put(builder.alloc, join.left.columns[ordinal].internal, {});
            for (join.right_keys) |program| for (program.required_columns) |ordinal| try needed.put(builder.alloc, join.right.columns[ordinal].internal, {});
            try projectScans(builder, join.left, needed);
            try projectScans(builder, join.right, needed);
        },
        .apply => |apply| {
            if (apply.condition) |program| for (program.required_columns) |ordinal| try needed.put(builder.alloc, node.columns[ordinal].internal, {});
            if (apply.demand) |program| for (program.required_columns) |ordinal| try needed.put(builder.alloc, apply.left.columns[ordinal].internal, {});
            try projectScans(builder, apply.right, needed);
            try projectScans(builder, apply.left, needed);
        },
        .query => |query| {
            var required = query.statement;
            // Transparent outputs retain ordinal slots, but cold forwarded
            // columns must not become physical scan dependencies.
            if (query.preserve_scope) required.columns = &.{};
            try markSelect(builder.alloc, needed, required);
            try projectScans(builder, query.source, needed);
        },
        .set => |set| {
            try projectScans(builder, set.left, needed);
            try projectScans(builder, set.right, needed);
        },
        .values => |arms| for (arms) |arm| try projectScans(builder, arm, needed),
    }
}

/// PostgreSQL owns an aggregate at the nearest query level referenced by its
/// arguments/FILTER. Until cross-level aggregate lifting is emitted, reject
/// an outer-owned aggregate rather than silently aggregating once per Apply.
/// Names are already bound here, so unqualified references and lexical
/// shadowing receive the same treatment as fully qualified references.
fn validateAggregateLevel(alloc: Allocator, columns: []const Column, input: *const ast.Scalar) anyerror!void {
    switch (input.*) {
        .column, .literal => {},
        .unary => |part| try validateAggregateLevel(alloc, columns, part.operand),
        .cast => |part| try validateAggregateLevel(alloc, columns, part.operand),
        .binary => |part| {
            try validateAggregateLevel(alloc, columns, part.left);
            try validateAggregateLevel(alloc, columns, part.right);
        },
        .case_when => |part| {
            for (part.branches) |branch| {
                try validateAggregateLevel(alloc, columns, branch.condition);
                try validateAggregateLevel(alloc, columns, branch.value);
            }
            if (part.otherwise) |value| try validateAggregateLevel(alloc, columns, value);
        },
        .in_list => |part| {
            try validateAggregateLevel(alloc, columns, part.operand);
            for (part.values) |value| try validateAggregateLevel(alloc, columns, value);
        },
        .call => |part| {
            const aggregates = @import("aggregate_binding.zig");
            if (part.window == null and (part.within_group != null or aggregates.aggregateKind(part.name) != null)) {
                // Preserve the ordinary grouping diagnostic for illegal
                // aggregate nesting; do not replace it with admission failure.
                for (part.args) |arg| if (aggregates.contains(arg)) return;
                if (part.filter) |filter| if (aggregates.contains(filter)) return;
                var names: std.StringHashMapUnmanaged(void) = .empty;
                for (part.args) |arg| try markExpression(alloc, &names, arg);
                if (part.filter) |filter| try markExpression(alloc, &names, filter);
                var level: ?u8 = null;
                for (columns) |column| if (names.contains(column.internal)) {
                    level = if (level) |prior| @min(prior, column.outer_level) else column.outer_level;
                };
                if (level != null and level.? != 0) return error.UnsupportedSqlShape;
                return;
            }
            for (part.args) |arg| try validateAggregateLevel(alloc, columns, arg);
            if (part.filter) |filter| try validateAggregateLevel(alloc, columns, filter);
        },
    }
}

pub fn bind(alloc: Allocator, backend: catalog.Backend, statement: ast.Select, parameters: []?ast.ColumnType) anyerror!Bound {
    return bindWithPreparedScope(alloc, backend, statement, parameters, null);
}

/// Only a mutation owner may supply this already-authorized scope. It is an
/// input relation of prepared target images and coherent captured source rows,
/// not a physical catalog name or an authorization shortcut for child scans.
pub fn bindPreparedScope(alloc: Allocator, backend: catalog.Backend, statement: ast.Select, parameters: []?ast.ColumnType, columns: []const Column) anyerror!Bound {
    return bindWithPreparedScope(alloc, backend, statement, parameters, columns);
}

fn bindWithPreparedScope(alloc: Allocator, backend: catalog.Backend, statement: ast.Select, parameters: []?ast.ColumnType, columns: ?[]const Column) anyerror!Bound {
    var builder: Builder = .{ .alloc = alloc, .backend = backend, .parameters = parameters, .prepared_scope = columns, .node_limit = if (statement.generated_values) 8192 else 256 };
    const normalized = try builder.lowerSubqueries(statement, &.{}, 0);
    if (std.mem.indexOfScalar(?ast.ColumnType, parameters, null) != null) {
        var shape: Builder = .{ .alloc = alloc, .backend = backend, .parameters = parameters, .identities = builder.identities, .shape_only = true, .prepared_scope = columns, .node_limit = if (statement.generated_values) 8192 else 256 };
        try shape.inferShape(normalized, &.{});
        builder.identities = shape.identities;
    }
    const root = try builder.querySource(normalized, &.{}, 0);
    const lowered = try builder.lower(root, normalized);
    var needed: std.StringHashMapUnmanaged(void) = .empty;
    try markSelect(alloc, &needed, lowered);
    try projectScans(&builder, root, &needed);
    const prepared_fields = try alloc.alloc([]const u8, builder.prepared_demands.count());
    var iterator = builder.prepared_demands.keyIterator();
    for (prepared_fields) |*field| field.* = iterator.next().?.*;
    return .{ .root = root, .scans = try builder.scans.toOwnedSlice(alloc), .table = try builder.virtualTable(root.columns), .statement = lowered, .prepared_fields = prepared_fields };
}

/// Assignment types cross arbitrary derived/CTE/set boundaries before binding
/// source programs. This is a catalog-only pass; it never opens a row reader.
pub fn inferExpected(alloc: Allocator, backend: catalog.Backend, statement: ast.Select, parameters: []?ast.ColumnType, expected: []const ast.ColumnType) !void {
    if (std.mem.indexOfScalar(?ast.ColumnType, parameters, null) == null) return;
    var shape: Builder = .{ .alloc = alloc, .backend = backend, .parameters = parameters, .shape_only = true, .node_limit = if (statement.generated_values) 8192 else 256 };
    try shape.inferShape(statement, expected);
}

/// Precise assignment domains flow through the same catalog-only origin and
/// scope analysis as SELECT inference; no source cursor or value is evaluated.
pub fn inferExpectedTypes(alloc: Allocator, backend: catalog.Backend, statement: ast.Select, parameters: []?ast.ColumnType, expected: []const scalar.Type) !void {
    if (std.mem.indexOfScalar(?ast.ColumnType, parameters, null) == null) return;
    var shape: Builder = .{ .alloc = alloc, .backend = backend, .parameters = parameters, .shape_only = true, .node_limit = if (statement.generated_values) 8192 else 256, .assignment_expected = expected };
    try shape.inferShape(statement, &.{});
}

/// Catalog-only output domain for mutation wildcard dependencies. The caller
/// keeps the authorized identity cache for the eventual executable binding;
/// no cursor, snapshot, row program or backend mutation is opened here.
pub fn projectionColumns(alloc: Allocator, backend: catalog.Backend, statement: ast.Select, parameters: []?ast.ColumnType) ![]const Column {
    var shape: Builder = .{ .alloc = alloc, .backend = backend, .parameters = parameters, .shape_only = true };
    try shape.inferShape(statement, &.{});
    return (try shape.querySource(statement, &.{}, 0)).columns;
}

/// Normalize one expression against an already-bound relation. Mutation arms
/// use the same qualified-name and ambiguity rules as SELECT, then bind typed
/// programs against the projected internal column ordinals.
pub fn lowerBoundExpression(alloc: Allocator, columns: []const Column, expression_: *const ast.Scalar) !*const ast.Scalar {
    var builder: Builder = .{ .alloc = alloc, .backend = undefined, .parameters = &.{} };
    return builder.expression(columns, expression_, &.{});
}

/// RETURNING has one authorized target, not a separate relational read. Reuse
/// ordinary qualification validation while retaining native column names for
/// evaluating the already-prepared mutation image.
pub fn normalizeTargetProjection(alloc: Allocator, backend: catalog.Backend, table: catalog.Table, name: ast.Name, aliased: bool, projections: []const ast.Projection) ![]const ast.Projection {
    var builder: Builder = .{ .alloc = alloc, .backend = backend, .parameters = &.{} };
    const columns = try alloc.alloc(Column, table.columns.len + 1);
    for (table.columns, columns[0..table.columns.len]) |column, *out| out.* = .{ .name = column.name, .internal = column.name, .qualifier = name.table, .scope = physicalScope(table, name, aliased), .type = column.type, .element_type = column.element_type, .numeric_modifier = column.numeric_modifier, .nullable = column.nullable };
    columns[table.columns.len] = .{ .name = "_id", .internal = "_id", .qualifier = name.table, .scope = physicalScope(table, name, aliased), .type = .string, .nullable = false, .visible = false };
    const source: Node = .{ .columns = columns, .operation = .singleton };
    return (try builder.lower(&source, .{ .table = name, .columns = projections })).columns;
}
