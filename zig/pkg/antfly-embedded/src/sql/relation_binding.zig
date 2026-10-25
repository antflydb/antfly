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
    temporal_kind: ?@import("temporal_value.zig").Kind = null,
    element_type: ?@import("array_value.zig").ElementType = null,
    numeric_modifier: ?scalar.NumericModifier = null,
    nullable: bool,
    visible: bool = true,
    /// Original USING keys remain available to qualified names and stars.
    unqualified_visible: bool = true,
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

/// Lowering passes resolve against the complete authorized lexical domain
/// before narrowing projection liveness; ambiguity is never guessed from names.
pub fn lookupColumn(columns: []const Column, name: []const u8) !Column {
    return Builder.field(columns, name);
}

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
        if (projection.field.len != 0 and count == before)
            return if (Builder.containsQualifier(columns, projection.field)) error.UndefinedColumn else error.UndefinedTable;
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
    const qualifier = if (projection.field.len != 0) projection.field else default_qualifier orelse return column.outer_level == 0 and column.unqualified_visible;
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
    return table.binding_scope orelse table.scope orelse if (name.namespace) |namespace| .{ .database = name.database orelse "", .namespace = namespace, .name = name.table, .revision = 0 } else null;
}

pub const Node = struct {
    pub const ConstantRef = struct { frame: usize, ordinal: usize };
    /// A bound ownership property, not a row estimate. Captured producers may
    /// never be restarted against an exhausted native statement cursor.
    pub const ReplayPolicy = enum {
        captured,
        restartable,
        streaming,

        fn combine(left: @This(), right: @This()) @This() {
            if (left == .captured or right == .captured) return .captured;
            return if (left == .streaming or right == .streaming) .streaming else .restartable;
        }
    };
    columns: []const Column,
    replay_policy: ReplayPolicy = .captured,
    security_barrier: bool = false,
    row_filter: ?catalog.ViewRowFilter = null,
    operation: union(enum) {
        singleton,
        recursive_ref: usize,
        outer_ref: usize,
        recursive: struct { id: usize, seed: *const Node, step: *const Node, all: bool },
        materialized_ref: *const Node,
        scan: struct {
            index: usize,
            source_columns: []const []const u8,
            // Keep bound ordinals stable without evaluating unreferenced
            // cells (including fallible virtual row-envelope conversions).
            demanded: ?[]bool = null,
        },
        unnest: struct { source: *const Node, arrays: []const scalar.Program, ordinality: bool },
        join: struct { output_order: ?[]const usize = null, estimated_rows: ?u64 = null, kind: ast.JoinKind, left: *const Node, right: *const Node, condition: ?scalar.Program, left_keys: []const scalar.Program, right_keys: []const scalar.Program, correlation: bool = false, membership: ?struct { correlations: usize } = null },
        apply: struct { id: usize, kind: ast.JoinKind, left: *const Node, right: *const Node, condition: ?scalar.Program, demand: ?scalar.Program = null, single_row: bool = false },
        query: struct {
            source: *const Node,
            statement: ast.Select,
            binding: describe.BoundStatement,
            preserve_scope: bool = false,
            /// Validated pure projection; no filter, bounds or effects. It
            /// may disappear only when none of its new identities is used.
            elidable_projection: bool = false,
            output_needed: ?[]const bool = null,
            constant_refs: []const ConstantRef = &.{},
        },
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
pub const ColumnDependency = struct { table_id: u64, column: catalog.Column, ordinal: u32, virtual: bool };
pub const Bound = struct { root: *const Node, scans: []const catalog.StatementScan, table: catalog.Table, statement: ast.Select, prepared_fields: []const []const u8 = &.{}, access_graph: ?*const @import("relation_access.zig").Graph = null, column_dependencies: []const ColumnDependency = &.{} };
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
        var result = self.backend;
        result.ptr = self;
        result.prepared_read = null;
        result.vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint };
        return result;
    }
    fn resolve(ptr: *anyopaque, _: Allocator, _: ast.Name, action: catalog.Action) !catalog.Table {
        if (action != .read) return error.UnsupportedSqlExecution;
        const self: *ResolveAdapter = @ptrCast(@alignCast(ptr));
        return self.table;
    }
    fn scan(_: *anyopaque, _: Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.UnsupportedSqlExecution;
    }
    fn mutate(_: *anyopaque, _: Allocator, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
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
    source_tables: std.StringHashMapUnmanaged(catalog.ReadRelation) = .empty,
    pub fn iface(self: *@This()) catalog.Backend {
        var result = self.backend;
        result.ptr = self;
        result.prepared_read = null;
        result.vtable = &.{ .resolve_read = resolveRead, .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint };
        return result;
    }
    fn resolve(ptr: *anyopaque, alloc: Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
        // This adapter is only used while binding the read side of a mutation.
        // Never let a future caller turn the pinned target into an implicit
        // authorization for a second write or an administrative operation.
        if (action != .read) return error.UnsupportedSqlExecution;
        const resolved = try resolveRead(ptr, alloc, name);
        return switch (resolved) {
            .table => |table| table,
            .view => error.UnsupportedSqlShape,
        };
    }
    fn resolveRead(ptr: *anyopaque, alloc: Allocator, name: ast.Name) !catalog.ReadRelation {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (std.mem.eql(u8, name.table, self.name.table) and std.mem.eql(u8, name.database orelse "", self.name.database orelse "") and std.mem.eql(u8, name.namespace orelse "", self.name.namespace orelse "")) return .{ .table = self.table };
        if (!self.cache_sources) return self.backend.resolveRead(alloc, name);
        const key = try std.fmt.allocPrint(alloc, "{s}\x00{s}\x00{s}", .{ name.database orelse "", name.namespace orelse "", name.table });
        if (self.source_tables.get(key)) |cached| {
            alloc.free(key);
            return cached;
        }
        errdefer alloc.free(key);
        const resolved = try self.backend.resolveRead(alloc, name);
        try self.source_tables.put(alloc, key, resolved);
        return resolved;
    }
    fn scan(_: *anyopaque, _: Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.InvalidSqlBackendResponse;
    }
    fn mutate(_: *anyopaque, _: Allocator, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
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

const MaterializedIdentity = struct {
    query: *const ast.Select,
    binding_scope: usize,
    outer_frame: ?usize,
};

const Builder = struct {
    alloc: Allocator,
    backend: catalog.Backend,
    parameters: []?ast.ColumnType,
    prepared_scope: ?[]const Column = null,
    prepared_demands: std.StringHashMapUnmanaged(void) = .empty,
    scans: std.ArrayList(catalog.StatementScan) = .empty,
    /// Definition binding records semantic references before scalar folding or
    /// scan pruning. Ordinary executable bindings allocate neither map.
    capture_dependencies: bool = false,
    dependency_origins: std.StringHashMapUnmanaged(ColumnDependency) = .empty,
    dependency_references: std.StringHashMapUnmanaged(void) = .empty,
    identities: std.StringHashMapUnmanaged(catalog.ReadRelation) = .empty,
    view_path: [64]u64 = undefined,
    view_depth: usize = 0,
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
    // A stored body is one immutable AST but can be bound at multiple lexical
    // correlation sites. Share a producer only within its binding authority
    // and defining outer frame, never by AST address alone.
    materialized_nodes: std.AutoHashMapUnmanaged(MaterializedIdentity, *const Node) = .empty,
    auto_materialized: std.AutoHashMapUnmanaged(*const ast.Select, void) = .empty,
    recursive_active: ?*const RecursiveFrame = null,
    recursive_next: usize = 0,
    outer_scope: ?*const OuterScope = null,
    outer_next: usize = 0,
    outer_used: [32]bool = @splat(false),
    assignment_expected: []const scalar.Type = &.{},
    const OuterScope = struct { id: usize, columns: []const Column, parent: ?*const OuterScope, cte_boundary: usize };
    const RecursiveFrame = struct { id: usize, query: *const ast.Select, columns: []const Column, parent: ?*const RecursiveFrame };
    const Constraint = struct { expression: *const ast.Scalar, expected: ?ast.ColumnType = null, expected_type: ?scalar.Type = null, type_mismatch: ?anyerror = null };

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
                if (@import("aggregate_binding.zig").collection(call)) {
                    const aggregate_binding = @import("aggregate_binding.zig");
                    if (call.window != null) return error.UnsupportedSqlShape;
                    if (call.within_group) |clause| if (!clause.ordinary) return error.SqlWrongAggregateKind;
                    const arity = try aggregate_binding.collectionArity(call);
                    const name = aggregate_binding.functionName(call.name);
                    if (!std.mem.eql(u8, name, "string_agg") and aggregate_binding.aggregateKind(name) == null) return error.UndefinedSqlFunction;
                    if (self.shape_only) {
                        for (call.args, 0..) |item, ordinal| try self.constraints.append(self.alloc, .{ .expression = try self.inferenceExpression(item, columns), .expected = if (std.mem.eql(u8, name, "string_agg") and ordinal < arity) .string else null });
                        if (call.filter) |filter| try self.constraints.append(self.alloc, .{ .expression = try self.inferenceExpression(filter, columns), .expected = .boolean });
                    }
                    const argument = try self.inferenceExpression(call.args[0], columns);
                    if (std.mem.eql(u8, name, "string_agg")) break :blk .{ .cast = .{ .operand = argument, .type = .string, .element_type = .text, .coercion = .function } };
                    if (std.mem.eql(u8, name, "array_agg")) break :blk .{ .call = .{ .name = "$array", .args = try self.alloc.dupe(*const ast.Scalar, &.{argument}) } };
                    var stripped = call;
                    stripped.args = call.args[0..arity];
                    stripped.within_group = null;
                    break :blk (try self.inferenceExpression(try self.scalarNode(.{ .call = stripped }), columns)).*;
                }
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
                        .row_number, .rank, .dense_rank => break :blk .{ .cast = .{ .operand = try self.scalarNode(.{ .literal = .{ .integer = 0 } }), .type = .integer, .element_type = .int64 } },
                        .ntile => {
                            if (call.args.len != 1) return error.InvalidSqlParameters;
                            if (self.shape_only) try self.constraints.append(self.alloc, .{ .expression = try self.inferenceExpression(call.args[0], columns), .expected_type = .{ .kind = .integer, .element_type = .int32 } });
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
                        .sum, .avg => sum: {
                            const input = try self.setType(argument, if (self.shape_only) self.shape_columns.items else try self.scalarColumns(columns));
                            if (input.kind == null) break :sum argument.*;
                            const output = if (kind == .sum) @import("operators.zig").Aggregate.sumType(input) else @import("operators.zig").Aggregate.averageType(input);
                            break :sum .{ .cast = .{ .operand = argument, .type = output.kind.?, .element_type = output.element_type } };
                        },
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
            const expression_ = try self.typedPredicate(source.columns, predicate_);
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
                if (!present) try shape.shape_columns.append(self.alloc, .{ .name = column.internal, .type = column.type, .element_type = column.element_type, .numeric_modifier = column.numeric_modifier, .temporal_kind = column.temporal_kind, .nullable = column.nullable });
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

    fn inferConstraint(self: *Builder, constraint: Constraint) !bool {
        return (if (constraint.expected_type) |descriptor|
            (self.backend.parameter_invocation orelse return error.UnsupportedSqlShape).infer(self.alloc, constraint.expression, self.shape_columns.items, self.parameters, descriptor, .{ .assignment = true })
        else
            scalar.inferParameters(self.alloc, constraint.expression, self.shape_columns.items, self.parameters, constraint.expected, .{ .invocation = self.backend.parameter_invocation })) catch |err| {
            if (constraint.type_mismatch) |mismatch| if (err == error.SqlTypeMismatch or err == error.SqlCannotCoerce) return mismatch;
            return err;
        };
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
                changed = try self.inferConstraint(constraint) or changed;
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
        for (leaves.items) |leaf| if (self.prepared.get(leaf).?.expressions.len != width) return error.SqlSetColumnCountMismatch;
        for (0..self.parameters.len + 2) |_| {
            @memset(common, .{});
            // All arms contribute before any unknown slot is constrained.
            for (leaves.items, 0..) |leaf, leaf_index| {
                const prepared = self.prepared.get(leaf).?;
                const columns = try self.scalarColumns(prepared.source.columns);
                for (prepared.expressions, common) |expression_, *kind| {
                    try mergeSetType(kind, try self.setType(expression_, columns), leaf_index == 0);
                }
            }
            var changed = false;
            for (leaves.items) |leaf| {
                const prepared = self.prepared.get(leaf).?;
                const columns = try self.scalarColumns(prepared.source.columns);
                if (prepared.lowered.predicate) |predicate_| {
                    const expression_ = try self.typedPredicate(prepared.source.columns, predicate_);
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
            // Concrete literals use the shared scalar identity resolver.
            // Dropping their width here silently widened VALUES/UNION int4
            // columns to int8, including correlated recursive seed columns.
            else => {},
        };
        return scalar.inferOutputWithInvocation(self.alloc, expression_, columns, self.parameters, self.backend.parameter_invocation);
    }

    fn resolveUnknown(kind: scalar.Type) scalar.Type {
        var resolved = kind;
        if (resolved.kind == null) resolved.kind = .string;
        return resolved;
    }

    fn mergeSetType(current: *scalar.Type, inferred: scalar.Type, first: bool) !void {
        return mergeInferredType(current, inferred, first) catch |err| switch (err) {
            error.SqlTypeMismatch => error.SqlSetTypeMismatch,
            else => err,
        };
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
        } else if (current.kind != inferred.kind or (current.kind == .datetime and (current.temporal_kind orelse .timestamp_tz) != (inferred.temporal_kind orelse .timestamp_tz))) return error.SqlTypeMismatch;
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
                    const expression_ = try self.typedPredicate(prepared.source.columns, predicate_);
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
        result.* = .{ .columns = columns, .operation = operation, .replay_policy = replayPolicy(operation) };
        return result;
    }
    fn repeatable(program: scalar.Program) bool {
        for (program.instructions) |instruction| if (instruction.operation == .call and instruction.operation.call.function.volatility() == .@"volatile") return false;
        return true;
    }
    fn replayPolicy(operation: @FieldType(Node, "operation")) Node.ReplayPolicy {
        return switch (operation) {
            .singleton, .literal_rows, .outer_ref => .restartable,
            .unnest => |function| blk: {
                if (function.source.replay_policy == .captured) break :blk .captured;
                for (function.arrays) |program| if (!repeatable(program)) break :blk .captured;
                break :blk .streaming;
            },
            .query => |query| blk: {
                if (query.binding.aggregate != null or query.binding.window != null or query.binding.order_keys.len != 0) break :blk .captured;
                if (query.binding.scalars.predicate) |program| if (!repeatable(program)) break :blk .captured;
                for (query.binding.scalars.projections) |optional| if (optional) |program| if (!repeatable(program)) break :blk .captured;
                break :blk query.source.replay_policy;
            },
            .join => |join| blk: {
                if (join.condition) |program| if (!repeatable(program)) break :blk .captured;
                for (join.left_keys) |program| if (!repeatable(program)) break :blk .captured;
                for (join.right_keys) |program| if (!repeatable(program)) break :blk .captured;
                break :blk join.left.replay_policy.combine(join.right.replay_policy);
            },
            .apply => |apply| blk: {
                if (apply.condition) |program| if (!repeatable(program)) break :blk .captured;
                if (apply.demand) |program| if (!repeatable(program)) break :blk .captured;
                break :blk apply.left.replay_policy.combine(apply.right.replay_policy);
            },
            // Explicit MATERIALIZED, recursive state and physical cursors
            // retain their existing replay owner. No storage is reopened.
            else => .captured,
        };
    }
    fn internal(self: *Builder) ![]const u8 {
        defer self.next_column += 1;
        return std.fmt.allocPrint(self.alloc, "$relation_{d}", .{self.next_column});
    }
    fn virtualTable(self: *Builder, columns: []const Column) !catalog.Table {
        const result = try self.alloc.alloc(catalog.Column, columns.len);
        for (columns, result) |column, *out| out.* = .{ .name = column.internal, .path = column.internal, .type = column.type, .element_type = column.element_type, .numeric_modifier = column.numeric_modifier, .temporal_kind = column.temporal_kind, .nullable = column.nullable };
        return .{ .id = 0, .physical_name = virtual_table_name, .schema_version = 0, .columns = result };
    }
    fn scalarColumns(self: *Builder, columns: []const Column) ![]const scalar.Column {
        const result = try self.alloc.alloc(scalar.Column, columns.len);
        for (columns, result) |column, *out| out.* = .{ .name = column.internal, .type = column.type, .element_type = column.element_type, .numeric_modifier = column.numeric_modifier, .temporal_kind = column.temporal_kind, .nullable = column.nullable };
        return result;
    }
    fn field(columns: []const Column, name: []const u8) !Column {
        var found: ?Column = null;
        for (columns) |column| {
            if (std.mem.eql(u8, column.name, name) and !column.unqualified_visible) continue;
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
        const column = field(columns, name) catch |err| {
            if (err == error.UndefinedColumn and name.len != 0 and name[0] != 0) if (std.mem.lastIndexOfScalar(u8, name, 0)) |separator| {
                const qualifier = name[0..separator];
                var present = containsQualifier(columns, qualifier);
                var outer = self.outer_scope;
                while (outer) |frame| : (outer = frame.parent) present = present or containsQualifier(frame.columns, qualifier);
                if (!present) return error.UndefinedTable;
            };
            return err;
        };
        try self.referenceColumn(column);
        for (0..32) |id| if (column.outer_dependencies & (@as(u32, 1) << @intCast(id)) != 0) {
            self.outer_used[id] = true;
        };
        if (column.outer_frame) |id| {
            self.outer_used[id] = true;
            if (self.outer_references) |references| try references.put(self.alloc, column.internal, {});
        }
        return column;
    }
    fn containsQualifier(columns: []const Column, qualifier: []const u8) bool {
        for (columns) |column| {
            if (qualifierMatches(column, qualifier)) return true;
            if (column.grouped_scope) |scope| if (containsQualifier(scope.*, qualifier)) return true;
        }
        return false;
    }
    fn referenceColumn(self: *Builder, column: Column) !void {
        if (self.capture_dependencies and self.dependency_origins.contains(column.internal))
            try self.dependency_references.put(self.alloc, column.internal, {});
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
    fn typedPredicate(self: *Builder, columns: []const Column, predicate_: *const ast.Predicate) !*const ast.Scalar {
        const descriptors: []scalar.Type = if (self.backend.parameter_invocation) |owner| owner.descriptors else &.{};
        return @import("bound_scalars.zig").predicateScalarTyped(self.alloc, try self.virtualTable(columns), predicate_, descriptors);
    }
    fn shapePredicate(self: *Builder, columns: []const Column, input: *const ast.Predicate) anyerror!*const ast.Scalar {
        return switch (input.*) {
            .scalar => |expression_| expression_,
            .comparison => |part| blk: {
                var kind: scalar.Type = .{};
                for (columns) |column| if (std.mem.eql(u8, column.internal, part.field)) {
                    kind = if (column.origin) |origin| try scalar.inferOutputWithInvocation(self.alloc, origin, self.shape_columns.items, self.parameters, self.backend.parameter_invocation) else scalar.columnType(column);
                };
                const descriptors: []const scalar.Type = if (self.backend.parameter_invocation) |owner| owner.descriptors else &.{};
                const right = try @import("bound_scalars.zig").comparisonOperand(self.alloc, kind, part.value, descriptors);
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
                try self.referenceColumn(column);
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
            wrapper.* = .{ .scalar = if (self.shape_only) try self.shapePredicate(source.columns, predicate_) else try self.typedPredicate(source.columns, predicate_) };
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
        if (arms.len == 1) return self.scalarNode(.{ .cast = .{ .operand = arms[0].columns[index].origin orelse return error.InvalidSqlBackendResponse, .type = common.kind.?, .element_type = common.element_type, .numeric_modifier = common.numeric_modifier, .temporal_kind = common.temporal_kind } });
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
        // PostgreSQL stores the analyzed WITH tree, including unused CTEs.
        // Bind their dependencies without making them executable row demand.
        if (self.capture_dependencies) for (statement.ctes, demand, 0..) |cte, references, index| {
            if (references != 0) continue;
            try self.backend.vtable.checkpoint(self.backend.ptr);
            const prefix = ctes[0 .. scope.len + index];
            if (cte.recursive and try @import("recursive_shape.zig").references(cte.query.*, cte.name, 0) != 0) {
                _ = try self.recursiveCte(cte, cte.name, prefix, depth + 1);
            } else {
                _ = try self.derived(cte.query, cte.name, cte.columns, prefix, depth + 1);
            }
        };
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
                        .temporal_kind = kind.temporal_kind,
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
                    column.temporal_kind = inferred.temporal_kind;
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
            if (left.columns.len != right.columns.len) return error.SqlSetColumnCountMismatch;
            const columns = try self.alloc.dupe(Column, left.columns);
            for (columns, right.columns) |*column, other| {
                if (self.shape_only) {
                    var common: scalar.Type = .{};
                    try mergeSetType(&common, try self.setType(column.origin.?, self.shape_columns.items), true);
                    try mergeSetType(&common, try self.setType(other.origin.?, self.shape_columns.items), false);
                    common = resolveUnknown(common);
                    const args = try self.alloc.dupe(*const ast.Scalar, &.{ try self.scalarNode(.{ .cast = .{ .operand = column.origin.?, .type = common.kind.?, .element_type = common.element_type, .numeric_modifier = common.numeric_modifier, .temporal_kind = common.temporal_kind } }), try self.scalarNode(.{ .cast = .{ .operand = other.origin.?, .type = common.kind.?, .element_type = common.element_type, .numeric_modifier = common.numeric_modifier, .temporal_kind = common.temporal_kind } }) });
                    column.origin = try self.scalarNode(.{ .call = .{ .name = "coalesce", .args = args } });
                    column.type = common.kind.?;
                    column.element_type = common.element_type;
                    column.numeric_modifier = common.numeric_modifier;
                    column.temporal_kind = common.temporal_kind;
                    try self.constraints.append(self.alloc, .{ .expression = column.origin.? });
                } else {
                    if (column.type != other.type or column.element_type != other.element_type) return error.SqlSetTypeMismatch;
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
                    .temporal_kind = output.temporal_kind,
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
                if (actual.kind != known or actual.element_type != kind.element_type or !scalar.NumericModifier.eql(actual.numeric_modifier, kind.numeric_modifier))
                    projection.expression = try self.scalarNode(.{ .cast = .{ .operand = projection.expression orelse try self.scalarNode(.{ .column = projection.field }), .type = known, .element_type = kind.element_type, .numeric_modifier = kind.numeric_modifier, .temporal_kind = kind.temporal_kind } });
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
            out.* = .{ .name = try self.alloc.dupe(u8, if (names.len == 0) column.name else names[index]), .internal = try self.internal(), .qualifier = alias, .type = column.type, .element_type = column.element_type, .numeric_modifier = column.numeric_modifier, .temporal_kind = column.temporal_kind, .nullable = true, .untyped_null = !resolve_unknown and untyped };
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

    fn prepareRecursiveTerm(self: *Builder, query: *const ast.Select, columns: []const Column, scope: []const ast.Cte, depth: usize) !void {
        // The nonrecursive term fixes the recursive attribute identities.
        // Resolve the UNION's common types before binding/evaluating the term,
        // then reuse ordinary prepared projection casts for implicit inputs.
        var leaves: std.ArrayList(*const ast.Select) = .empty;
        try self.collectSet(query, scope, &leaves, depth + 1);
        const prepared = self.prepared.get(query) orelse return error.InvalidSqlBackendResponse;
        if (prepared.expressions.len != columns.len) return error.SqlTypeMismatch;
        const inputs = try self.scalarColumns(prepared.source.columns);
        for (prepared.expressions, prepared.types, columns) |expression_, *target, column| {
            const seed: scalar.Type = .{ .kind = column.type, .element_type = scalar.columnElementType(column.type, column.element_type), .numeric_modifier = column.numeric_modifier, .temporal_kind = column.temporal_kind, .nullable = true };
            var common = seed;
            mergeInferredType(&common, try self.setType(expression_, inputs), false) catch |err| switch (err) {
                error.SqlCannotCoerce, error.SqlTypeMismatch => return error.SqlRecursiveTypeMismatch,
                else => return err,
            };
            if (common.kind != seed.kind or common.element_type != seed.element_type or !scalar.NumericModifier.eql(common.numeric_modifier, seed.numeric_modifier)) return error.SqlRecursiveTypeMismatch;
            target.* = seed;
            _ = try scalar.inferParameters(self.alloc, expression_, inputs, self.parameters, seed.kind, .{ .invocation = self.backend.parameter_invocation });
        }
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
        if (!self.shape_only) try self.prepareRecursiveTerm(set.right, seed.columns, step_scope, depth + 1);
        const step = try self.derived(set.right, cte.name, cte.columns, step_scope, depth + 1);
        if (seed.columns.len != step.columns.len) return error.SqlTypeMismatch;
        const columns = try self.alloc.dupe(Column, seed.columns);
        for (columns, step.columns) |*column, other| {
            column.nullable = true;
            if (self.shape_only) {
                const args = try self.alloc.dupe(*const ast.Scalar, &.{ column.origin.?, other.origin.? });
                column.origin = try self.scalarNode(.{ .call = .{ .name = "coalesce", .args = args } });
                try self.constraints.append(self.alloc, .{ .expression = column.origin.?, .type_mismatch = error.SqlRecursiveTypeMismatch });
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
            .unnest => |function| blk: {
                // PostgreSQL's zipped multi-array form is grammar sugar, not
                // an overload of the schema-qualified single-array function.
                if (function.qualified and function.args.len != 1) return error.SqlUndefinedFunction;
                if (function.args.len == 0 or function.args.len > 1024) return error.SqlProgramLimitExceeded;
                const width = function.args.len + @intFromBool(function.ordinality);
                if (function.columns.len > width) return error.InvalidSqlParameters;
                const source = try self.withOuter(try self.node(&.{}, .singleton));
                const types = try self.scalarColumns(source.columns);
                const columns = try self.alloc.alloc(Column, width);
                const programs = try self.alloc.alloc(scalar.Program, if (self.shape_only) 0 else function.args.len);
                for (function.args, columns[0..function.args.len], 0..) |arg, *column, index| {
                    const expression_ = try self.expression(source.columns, arg, &.{});
                    const symbolic = if (self.shape_only) try self.inferenceExpression(expression_, source.columns) else expression_;
                    const output = try scalar.inferOutputWithInvocation(self.alloc, symbolic, if (self.shape_only) self.shape_columns.items else types, self.parameters, self.backend.parameter_invocation);
                    if (output.kind == null) return error.SqlAmbiguousFunction;
                    if (output.kind != .array) return error.SqlUndefinedFunction;
                    const element = output.element_type orelse return error.SqlAmbiguousFunction;
                    const kind: ast.ColumnType = switch (element) {
                        .text => .string,
                        .uuid => .uuid,
                        .int16, .int32, .int64 => .integer,
                        .float32, .float64, .numeric => .number,
                        .boolean => .boolean,
                        .jsonb => .json,
                    };
                    column.* = .{ .name = if (index < function.columns.len) function.columns[index] else if (function.args.len == 1) function.alias orelse "unnest" else "unnest", .internal = try self.internal(), .qualifier = function.alias orelse "unnest", .type = kind, .element_type = element, .nullable = true, .origin = if (self.shape_only) try self.scalarNode(.{ .cast = .{ .type = kind, .element_type = element, .operand = try self.scalarNode(.{ .literal = .null }) } }) else null };
                    if (self.shape_only) {
                        try self.constraints.append(self.alloc, .{ .expression = symbolic, .expected = .array });
                    } else {
                        programs[index] = try scalar.bindExpectedWithSettings(self.alloc, expression_, types, self.parameters, .array, .{ .invocation = self.backend.parameter_invocation }, self.backend.settings_view);
                    }
                }
                if (function.ordinality) columns[width - 1] = .{ .name = if (function.columns.len == width) function.columns[width - 1] else "ordinality", .internal = try self.internal(), .qualifier = function.alias orelse "unnest", .type = .integer, .element_type = .int64, .nullable = false, .origin = if (self.shape_only) try self.scalarNode(.{ .cast = .{ .type = .integer, .operand = try self.scalarNode(.{ .literal = .null }) } }) else null };
                break :blk try self.node(columns, if (self.shape_only) .singleton else .{ .unnest = .{ .source = source, .arrays = programs, .ordinality = function.ordinality } });
            },
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
                        if (self.shape_only) try self.shape_columns.append(self.alloc, .{ .name = column.internal, .type = column.type, .element_type = column.element_type, .numeric_modifier = column.numeric_modifier, .temporal_kind = column.temporal_kind, .nullable = column.nullable });
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
                                const materialized_identity: MaterializedIdentity = .{
                                    .query = cte.query,
                                    .binding_scope = self.backend.binding_scope,
                                    .outer_frame = if (self.outer_scope) |frame| frame.id else null,
                                };
                                const materialized = self.materialized_nodes.get(materialized_identity) orelse source: {
                                    const source = try self.derived(cte.query, cte.name, cte.columns, scope[0..i], depth + 1);
                                    try self.materialized_nodes.put(self.alloc, materialized_identity, source);
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
                const identity = try std.fmt.allocPrint(self.alloc, "{d}\x00{s}\x00{s}\x00{s}", .{ self.backend.binding_scope, reference.name.database orelse "", reference.name.namespace orelse "", reference.name.table });
                const entry = try self.identities.getOrPut(self.alloc, identity);
                if (!entry.found_existing) entry.value_ptr.* = try self.backend.resolveRead(self.alloc, reference.name);
                const resolved = entry.value_ptr.*;
                if (resolved == .view) {
                    if (reference.search != null or reference.mutation_target or reference.prepared_rows) return error.UnsupportedSqlShape;
                    const view = resolved.view;
                    if (view.id == 0 or view.generation == 0 or view.backend.binding_scope == 0 or self.view_depth >= self.view_path.len) return error.InvalidSqlBackendResponse;
                    for (self.view_path[0..self.view_depth]) |id| if (id == view.id) return error.SqlViewCycle;
                    const previous_backend = self.backend;
                    const previous_outer = self.outer_scope;
                    self.view_path[self.view_depth] = view.id;
                    self.view_depth += 1;
                    // Change only name-resolution authority. The invocation,
                    // clock, regex budget and caller settings belong to the
                    // enclosing statement, not the stored view provider.
                    self.backend.ptr = view.backend.ptr;
                    self.backend.vtable = view.backend.vtable;
                    self.backend.binding_scope = view.backend.binding_scope;
                    self.backend.settings_view = previous_backend.settings_view orelse view.backend.settings_view;
                    self.backend.prepared_read = null;
                    self.outer_scope = null;
                    defer {
                        self.backend = previous_backend;
                        self.outer_scope = previous_outer;
                        self.view_depth -= 1;
                    }
                    const names = try self.alloc.alloc([]const u8, view.columns.len);
                    for (view.columns, names) |column, *name| name.* = column.name;
                    // A stored view has its own lexical scope: caller CTEs,
                    // lateral frames and correlation may never enter its body.
                    var result = try self.derived(view.query, reference.alias orelse reference.name.table, names, &.{}, depth + 1);
                    if (result.columns.len != view.columns.len) return error.InvalidSqlBackendResponse;
                    for (result.columns, view.columns) |actual, expected| {
                        const column: catalog.Column = .{ .name = actual.name, .path = actual.name, .type = actual.type, .element_type = actual.element_type, .numeric_modifier = actual.numeric_modifier, .temporal_kind = actual.temporal_kind };
                        if (!std.mem.eql(u8, &try column.identitySignature(self.alloc), &try expected.identitySignature(self.alloc))) return error.CatalogGenerationChanged;
                    }
                    // Authorization uses the complete view output before a
                    // parent's frozen exposure narrows/relabels its columns.
                    @constCast(result).row_filter = view.row_filter;
                    if (view.exposure) |exposure| {
                        if (exposure.len == 0) return error.InvalidSqlBackendResponse;
                        const projections = try self.alloc.alloc(ast.Projection, exposure.len);
                        const columns = try self.alloc.alloc(Column, exposure.len);
                        for (exposure, projections, columns) |item, *projection, *column| {
                            if (item.ordinal >= result.columns.len) return error.InvalidSqlBackendResponse;
                            const forwarded = result.columns[item.ordinal];
                            projection.* = .{ .field = forwarded.internal, .alias = item.name };
                            column.* = forwarded;
                            column.name = item.name;
                            column.internal = try self.internal();
                        }
                        const selection: ast.Select = .{ .table = .{ .table = "$view_output" }, .columns = projections };
                        var adapter: ResolveAdapter = .{ .backend = self.backend, .table = try self.virtualTable(result.columns) };
                        const compiled: compiler.Compiled = .{ .arena = undefined, .statement = .{ .select = selection }, .parameter_count = @intCast(self.parameters.len) };
                        const bound = try describe.bindInternal(self.alloc, adapter.iface(), &compiled, self.parameters);
                        result = try self.node(columns, .{ .query = .{ .source = result, .statement = selection, .binding = bound } });
                    }
                    @constCast(result).security_barrier = view.security_barrier or result.row_filter != null;
                    if (reference.alias == null) for (@constCast(result.columns)) |*column| {
                        column.scope = .{ .database = reference.name.database orelse "", .namespace = reference.name.namespace orelse "", .name = reference.name.table, .revision = 0 };
                    };
                    break :blk result;
                }
                var table = resolved.table;
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
                const virtual_columns = table.virtualColumns();
                const virtual_count = virtual_columns.len;
                const columns = try self.alloc.alloc(Column, table.columns.len + virtual_count + metadata_count);
                const source_columns = try self.alloc.alloc([]const u8, columns.len);
                const fields = try self.alloc.alloc([]const u8, table.columns.len);
                for (table.columns, columns[0..table.columns.len], source_columns[0..table.columns.len], fields, 0..) |column, *out, *source_name, *field_name, position| {
                    out.* = .{ .name = try self.alloc.dupe(u8, column.name), .internal = try self.internal(), .qualifier = reference.alias orelse reference.name.table, .scope = physicalScope(table, reference.name, reference.alias != null), .type = column.type, .element_type = column.element_type, .numeric_modifier = column.numeric_modifier, .temporal_kind = column.temporal_kind, .nullable = column.nullable };
                    source_name.* = column.name;
                    field_name.* = column.path;
                    if (self.capture_dependencies) try self.dependency_origins.put(self.alloc, out.internal, .{ .table_id = table.id, .column = column, .ordinal = @intCast(position), .virtual = false });
                }
                for (virtual_columns, table.columns.len..) |column, position| {
                    columns[position] = .{ .name = column.name, .internal = try self.internal(), .qualifier = reference.alias orelse reference.name.table, .scope = physicalScope(table, reference.name, reference.alias != null), .type = column.type, .element_type = column.element_type, .nullable = column.nullable, .visible = reference.search != null and std.mem.eql(u8, column.name, "_id") };
                    source_columns[position] = column.name;
                    if (self.capture_dependencies) try self.dependency_origins.put(self.alloc, columns[position].internal, .{ .table_id = table.id, .column = column, .ordinal = @intCast(position), .virtual = true });
                }
                for (@import("joined_mutation.zig").metadata_fields[0..metadata_count], 0..) |name, i| {
                    columns[table.columns.len + virtual_count + i] = .{ .name = name, .internal = try self.internal(), .qualifier = reference.alias orelse reference.name.table, .type = if (i == 2) .json else .string, .nullable = i == 2, .visible = false };
                    source_columns[table.columns.len + virtual_count + i] = name;
                }
                if (self.shape_only) for (columns) |column| try self.shape_columns.append(self.alloc, .{ .name = column.internal, .type = column.type, .element_type = column.element_type, .numeric_modifier = column.numeric_modifier, .temporal_kind = column.temporal_kind, .nullable = column.nullable });
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
                            column.unqualified_visible = source_column.unqualified_visible;
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
                const lateral = join.right.* == .unnest or (join.right.* == .derived and join.right.derived.lateral);
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
                for (left.columns) |a| for (right.columns) |b| if (a.qualifier.len != 0 and std.mem.eql(u8, a.qualifier, b.qualifier)) return error.DuplicateSqlAlias;
                if (join.keys != .none) {
                    if (join.condition != null or join.membership != null or join.demand != null or join.single_row or join.kind == .cross) return error.InvalidSqlBackendResponse;
                    break :blk try self.keyJoin(left, right, join.kind, join.keys, join.using_alias, apply_id);
                }
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
                if (!self.shape_only and demand != null) {
                    const mutable = @constCast(result);
                    mutable.operation.apply.demand = demand;
                    mutable.replay_policy = replayPolicy(mutable.operation);
                }
                break :blk result;
            },
        };
    }
    fn keyColumn(columns: []const Column, name: []const u8, visible_only: bool) !Column {
        var found: ?Column = null;
        for (columns) |column| {
            // Explicit USING can name an addressable hidden metadata column;
            // NATURAL discovery, like plain *, only considers visible cells.
            if ((visible_only and !column.visible) or !column.unqualified_visible or !std.mem.eql(u8, column.name, name)) continue;
            if (found != null) return error.AmbiguousSqlColumn;
            found = column;
        }
        return found orelse error.UndefinedColumn;
    }

    fn keyJoin(self: *Builder, left: *const Node, right: *const Node, kind: ast.JoinKind, keys: @FieldType(@FieldType(ast.Relation, "join"), "keys"), alias: ?[]const u8, apply_id: ?usize) !*const Node {
        if (alias) |name| {
            for (left.columns) |column| if (std.mem.eql(u8, column.qualifier, name)) return error.DuplicateSqlAlias;
            for (right.columns) |column| if (std.mem.eql(u8, column.qualifier, name)) return error.DuplicateSqlAlias;
        }
        var natural: std.ArrayList([]const u8) = .empty;
        const names = switch (keys) {
            .none => unreachable,
            .using => |names| names,
            .natural => blk: {
                for (left.columns) |column| {
                    if (!column.visible or !column.unqualified_visible) continue;
                    _ = keyColumn(right.columns, column.name, true) catch |err| {
                        if (err == error.UndefinedColumn) continue;
                        return err;
                    };
                    try natural.append(self.alloc, column.name);
                }
                break :blk natural.items;
            },
        };
        var condition: ?*const ast.Scalar = null;
        const merged = try self.alloc.alloc(*const ast.Scalar, names.len);
        for (names, merged, 0..) |name, *value, index| {
            for (names[0..index]) |prior| if (std.mem.eql(u8, prior, name)) return error.DuplicateSqlColumn;
            const l = try keyColumn(left.columns, name, keys == .natural);
            const r = try keyColumn(right.columns, name, keys == .natural);
            try self.referenceColumn(l);
            try self.referenceColumn(r);
            const l_expr = try self.scalarNode(.{ .column = l.internal });
            const r_expr = try self.scalarNode(.{ .column = r.internal });
            const equal = try self.scalarNode(.{ .binary = .{ .op = .eq, .left = l_expr, .right = r_expr } });
            condition = if (condition) |prior| try self.scalarNode(.{ .binary = .{ .op = .@"and", .left = prior, .right = equal } }) else equal;
            value.* = try self.scalarNode(.{ .call = .{ .name = "coalesce", .args = try self.alloc.dupe(*const ast.Scalar, &.{ l_expr, r_expr }) } });
        }
        const source = try self.joinNode(left, right, kind, condition, apply_id);
        if (names.len == 0) return source;
        const columns = try self.alloc.alloc(Column, names.len + source.columns.len);
        const projections = try self.alloc.alloc(ast.Projection, columns.len);
        const types = try self.scalarColumns(source.columns);
        for (names, merged, columns[0..names.len], projections[0..names.len]) |name, expression_, *column, *projection| {
            const symbolic = if (self.shape_only) try self.inferenceExpression(expression_, source.columns) else expression_;
            const output = try scalar.inferOutputWithInvocation(self.alloc, symbolic, if (self.shape_only) self.shape_columns.items else types, self.parameters, self.backend.parameter_invocation);
            column.* = .{ .name = name, .internal = try self.internal(), .qualifier = alias orelse "", .type = output.kind orelse .string, .element_type = output.element_type, .numeric_modifier = output.numeric_modifier, .temporal_kind = output.temporal_kind, .nullable = true, .origin = if (self.shape_only) symbolic else null };
            projection.* = .{ .expression = expression_, .alias = column.internal };
        }
        for (source.columns, columns[names.len..], projections[names.len..]) |input, *column, *projection| {
            column.* = input;
            for (names) |name| if (std.mem.eql(u8, input.name, name)) {
                column.unqualified_visible = false;
                break;
            };
            projection.* = .{ .field = input.internal, .alias = input.internal };
        }
        if (self.shape_only) return self.node(columns, .singleton);
        const statement: ast.Select = .{ .table = .{ .table = "$sql_relation" }, .columns = projections, .internal_projection = true };
        var adapter: ResolveAdapter = .{ .backend = self.backend, .table = try self.virtualTable(source.columns) };
        const compiled: compiler.Compiled = .{ .arena = undefined, .statement = .{ .select = statement }, .parameter_count = @intCast(self.parameters.len) };
        const bound = try describe.bindInternal(self.alloc, adapter.iface(), &compiled, self.parameters);
        return self.node(columns, .{ .query = .{ .source = source, .statement = statement, .binding = bound, .preserve_scope = true, .elidable_projection = true } });
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
            mergeInferredType(&common, if (right_column.untyped_null) .{} else .{ .kind = right_column.type, .element_type = right_column.element_type, .numeric_modifier = right_column.numeric_modifier, .temporal_kind = right_column.temporal_kind, .nullable = right_column.nullable }, false) catch |err| return switch (err) {
                error.SqlTypeMismatch, error.SqlCannotCoerce => error.SqlUndefinedOperator,
                else => err,
            };
            common = resolveUnknown(common);
            const l = try self.scalarNode(.{ .cast = .{ .operand = left_expression, .type = common.kind.?, .element_type = common.element_type, .numeric_modifier = common.numeric_modifier, .temporal_kind = common.temporal_kind } });
            const r = try self.scalarNode(.{ .cast = .{ .operand = right_expression, .type = common.kind.?, .element_type = common.element_type, .numeric_modifier = common.numeric_modifier, .temporal_kind = common.temporal_kind } });
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
            .unnest => |function| try self.uncorrelated(function.source, id),
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
        const left_columns = try self.scalarColumns(left.columns);
        const right_columns = try self.scalarColumns(right.columns);
        const left_type = try scalar.inferOutputWithInvocation(self.alloc, left_node, left_columns, self.parameters, self.backend.parameter_invocation);
        const right_type = try scalar.inferOutputWithInvocation(self.alloc, right_node, right_columns, self.parameters, self.backend.parameter_invocation);
        const numeric_pair = (left_type.kind == .integer or left_type.kind == .number) and
            (right_type.kind == .integer or right_type.kind == .number);
        if (numeric_pair) {
            const domain = try scalar.comparisonDomain(left_type, right_type);
            const element = scalar.columnElementType(domain.kind.?, domain.element_type);
            // Keep identity operands bare: native exact key spooling,
            // scan-filter lineage and join partials depend on this shape.
            if (left_type.kind != domain.kind or scalar.columnElementType(left_type.kind.?, left_type.element_type) != element)
                left_node = try self.scalarNode(.{ .cast = .{ .operand = left_node, .type = domain.kind.?, .element_type = domain.element_type } });
            if (right_type.kind != domain.kind or scalar.columnElementType(right_type.kind.?, right_type.element_type) != element)
                right_node = try self.scalarNode(.{ .cast = .{ .operand = right_node, .type = domain.kind.?, .element_type = domain.element_type } });
        } else if (left_type.kind != right_type.kind or scalar.columnElementType(left_type.kind orelse return, left_type.element_type) !=
            scalar.columnElementType(right_type.kind orelse return, right_type.element_type))
        {
            // A residual equality is always authoritative. Do not install a
            // false-negative hash prefilter for an unproven mixed domain.
            return;
        }
        try left_keys.append(self.alloc, try scalar.bindWithSettings(self.alloc, left_node, left_columns, self.parameters, .{ .invocation = self.backend.parameter_invocation }, self.backend.settings_view));
        try right_keys.append(self.alloc, try scalar.bindWithSettings(self.alloc, right_node, right_columns, self.parameters, .{ .invocation = self.backend.parameter_invocation }, self.backend.settings_view));
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
    // Lowering preserves named-window input demand even for an unused
    // definition. Its staged scalar subqueries retain cardinality errors.
    for (statement.windows) |definition| {
        for (definition.window.partition) |expression| try markExpression(alloc, needed, expression);
        for (definition.window.order) |order| if (order.expression) |expression| try markExpression(alloc, needed, expression) else if (order.position == null) try needed.put(alloc, order.field, {});
    }
    for (statement.order_by) |order| if (order.expression) |expression| {
        try markExpression(alloc, needed, expression);
    } else if (order.position == null) {
        try needed.put(alloc, order.field, {});
    };
}
fn projectScans(builder: *Builder, node: *const Node, needed: *std.StringHashMapUnmanaged(void)) anyerror!void {
    // Authorization consumes the complete declared view row before caller
    // projection. Cold-column pruning must not replace policy inputs by NULL.
    if (node.row_filter != null) for (node.columns) |column| try needed.put(builder.alloc, column.internal, {});
    switch (node.operation) {
        .unnest => |function| {
            for (function.arrays) |program| for (program.required_columns) |ordinal| try needed.put(builder.alloc, function.source.columns[ordinal].internal, {});
            try projectScans(builder, function.source, needed);
        },
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
            for (recursive.seed.columns) |column| try needed.put(builder.alloc, column.internal, {});
            for (recursive.step.columns) |column| try needed.put(builder.alloc, column.internal, {});
            // The working row is positional. Every recursive output is needed
            // to seed the next iteration even when the outer SELECT projects less.
            try projectScans(builder, recursive.seed, needed);
            try projectScans(builder, recursive.step, needed);
        },
        .scan => |scan| {
            var fields: std.ArrayList([]const u8) = .empty;
            const request = &builder.scans.items[scan.index];
            const demanded = scan.demanded orelse blk: {
                const mask = try builder.alloc.alloc(bool, node.columns.len);
                @memset(mask, false);
                @constCast(node).operation.scan.demanded = mask;
                break :blk mask;
            };
            for (node.columns, scan.source_columns, demanded) |column, name, *required| {
                required.* = required.* or needed.contains(column.internal);
                if (!required.* or std.mem.eql(u8, name, "_id") or @import("joined_mutation.zig").isMetadata(name)) continue;
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
            const decisions = @import("decision_eval.zig");
            const prunable = blk: {
                if (query.binding.aggregate != null or query.binding.window != null or query.statement.count_all or query.binding.order_keys.len != 0) break :blk false;
                for (query.binding.scalars.projections) |*optional| if (optional.*) |*program| if (decisions.hasExternal(program) or !Builder.repeatable(program.*)) break :blk false;
                break :blk true;
            };
            const grouped_prunable = blk: {
                const aggregate = query.binding.aggregate orelse break :blk false;
                if (decisions.hasExternalPrograms(aggregate.outputs) or decisions.hasExternalPrograms(aggregate.orders)) break :blk false;
                for (aggregate.outputs) |program| if (!Builder.repeatable(program)) break :blk false;
                for (aggregate.ordered) |ordered| if (ordered.direct) |program| {
                    if (decisions.hasExternal(&program) or !Builder.repeatable(program)) break :blk false;
                };
                // Volatile arguments remain observable even when their result
                // column is unused (for example SUM(nextval(...))).
                for (aggregate.input.projections) |optional| if (optional) |program| {
                    if (decisions.hasExternal(&program) or !Builder.repeatable(program)) break :blk false;
                };
                break :blk true;
            };
            if (grouped_prunable) {
                const outputs = try builder.alloc.alloc(bool, node.columns.len);
                for (outputs, node.columns) |*output, column| output.* = needed.contains(column.internal);
                @constCast(node).operation.query.output_needed = outputs;
                const original = query.binding.aggregate.?;
                const active = if (std.mem.allEqual(bool, outputs, true)) original else active: {
                    const projected = try builder.alloc.create(@import("aggregate_binding.zig").Bound);
                    projected.* = try @import("aggregate_binding.zig").project(builder.alloc, original, outputs);
                    @constCast(node).operation.query.binding.aggregate = projected;
                    @constCast(node).operation.query.binding.scalars = projected.input;
                    break :active projected;
                };
                for (active.input.required) |ordinal| try needed.put(builder.alloc, active.input.columns[ordinal].name, {});
                const Constants = struct {
                    fn mark(alloc: Allocator, map: *std.StringHashMapUnmanaged(void), program: *const scalar.Program, offset: usize, names: []const []const u8) !void {
                        for (program.required_columns) |ordinal| if (ordinal >= offset) {
                            const index = ordinal - offset;
                            if (index >= names.len) return error.InvalidSqlBackendResponse;
                            try map.put(alloc, names[index], {});
                        };
                    }
                };
                // Global aggregates read correlated values from their Apply
                // frame even when their physical input is empty. These live
                // constants are outside the row-input projection domain.
                const constant_offset = active.group_count + active.specs.len;
                for (active.outputs) |*program| try Constants.mark(builder.alloc, needed, program, constant_offset, query.statement.invocation_constants);
                for (active.orders) |*program| try Constants.mark(builder.alloc, needed, program, constant_offset, query.statement.invocation_constants);
                if (active.having) |*program| try Constants.mark(builder.alloc, needed, program, constant_offset, query.statement.invocation_constants);
                // The active typed inputs already include grouping, HAVING,
                // ordering and live reducer dependencies. Visiting the old
                // projection AST would resurrect discarded argument programs.
                required.columns = &.{};
            } else if (prunable) {
                const outputs = try builder.alloc.alloc(bool, node.columns.len);
                for (outputs, node.columns, query.statement.columns) |*output, column, projection| {
                    output.* = needed.contains(column.internal);
                    if (!output.*) continue;
                    if (projection.expression) |expression| try markExpression(builder.alloc, needed, expression) else try needed.put(builder.alloc, projection.field, {});
                }
                @constCast(node).operation.query.output_needed = outputs;
                required.columns = &.{};
            } else if (query.preserve_scope) {
                for (query.statement.columns, node.columns) |projection, column| {
                    if (!needed.contains(column.internal)) continue;
                    if (projection.expression) |expression_| try markExpression(builder.alloc, needed, expression_);
                }
                required.columns = &.{};
            }
            try markSelect(builder.alloc, needed, required);
            if (prunable) {
                var ordinals: std.ArrayList(u32) = .empty;
                for (query.binding.scalars.required) |ordinal| if (needed.contains(query.binding.scalars.columns[ordinal].name)) try ordinals.append(builder.alloc, ordinal);
                @constCast(node).operation.query.binding.scalars.required = try ordinals.toOwnedSlice(builder.alloc);
            }
            try projectScans(builder, query.source, needed);
        },
        .set => |set| {
            for (set.left.columns) |column| try needed.put(builder.alloc, column.internal, {});
            for (set.right.columns) |column| try needed.put(builder.alloc, column.internal, {});
            try projectScans(builder, set.left, needed);
            try projectScans(builder, set.right, needed);
        },
        .values => |arms| for (arms) |arm| {
            for (arm.columns) |column| try needed.put(builder.alloc, column.internal, {});
            try projectScans(builder, arm, needed);
        },
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
            if (part.window == null and (aggregates.collection(part) or part.within_group != null or aggregates.aggregateKind(part.name) != null)) {
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

/// Stored definitions need semantic dependencies, not only the physical
/// projection left after optimization. This uses the executable name binder.
pub fn bindDefinition(alloc: Allocator, backend: catalog.Backend, statement: ast.Select) anyerror!Bound {
    return bindWithPurpose(alloc, backend, statement, &.{}, null, true);
}

/// Only a mutation owner may supply this already-authorized scope. It is an
/// input relation of prepared target images and coherent captured source rows,
/// not a physical catalog name or an authorization shortcut for child scans.
pub fn bindPreparedScope(alloc: Allocator, backend: catalog.Backend, statement: ast.Select, parameters: []?ast.ColumnType, columns: []const Column) anyerror!Bound {
    return bindWithPreparedScope(alloc, backend, statement, parameters, columns);
}

fn bindWithPreparedScope(alloc: Allocator, backend: catalog.Backend, statement: ast.Select, parameters: []?ast.ColumnType, columns: ?[]const Column) anyerror!Bound {
    return bindWithPurpose(alloc, backend, statement, parameters, columns, false);
}

fn bindWithPurpose(alloc: Allocator, backend: catalog.Backend, statement: ast.Select, parameters: []?ast.ColumnType, columns: ?[]const Column, capture_dependencies: bool) anyerror!Bound {
    var builder: Builder = .{ .alloc = alloc, .backend = backend, .parameters = parameters, .prepared_scope = columns, .capture_dependencies = capture_dependencies, .node_limit = if (statement.generated_values) 8192 else 256 };
    const normalized = try builder.lowerSubqueries(statement, &.{}, 0);
    if (std.mem.indexOfScalar(?ast.ColumnType, parameters, null) != null) {
        var shape: Builder = .{ .alloc = alloc, .backend = backend, .parameters = parameters, .identities = builder.identities, .shape_only = true, .prepared_scope = columns, .node_limit = if (statement.generated_values) 8192 else 256 };
        try shape.inferShape(normalized, &.{});
        builder.identities = shape.identities;
    }
    var root = try builder.querySource(normalized, &.{}, 0);
    const lowered = try builder.lower(root, normalized);
    var needed: std.StringHashMapUnmanaged(void) = .empty;
    try markSelect(alloc, &needed, lowered);
    // At the statement boundary all scalar names have already been bound.
    // Discard unused synthetic USING cells for count/qualified-only queries
    // without altering any nested join's ordinal layout or skipping binding.
    while (root.row_filter == null and root.operation == .query and root.operation.query.elidable_projection) {
        const source = root.operation.query.source;
        const required = for (root.columns) |column| {
            if (needed.contains(column.internal) and !hasInternal(source.columns, column.internal)) break true;
        } else false;
        if (required) break;
        root = source;
    }
    try projectScans(&builder, root, &needed);
    const prepared_fields = try alloc.alloc([]const u8, builder.prepared_demands.count());
    var iterator = builder.prepared_demands.keyIterator();
    for (prepared_fields) |*field| field.* = iterator.next().?.*;
    const scans = try builder.scans.toOwnedSlice(alloc);
    const dependencies = try alloc.alloc(ColumnDependency, builder.dependency_references.count());
    var references = builder.dependency_references.keyIterator();
    for (dependencies) |*dependency| dependency.* = builder.dependency_origins.get(references.next().?.*).?;
    return .{ .root = root, .scans = scans, .table = try builder.virtualTable(root.columns), .statement = lowered, .prepared_fields = prepared_fields, .access_graph = try @import("relation_access.zig").prepareGraph(alloc, root, scans, backend), .column_dependencies = dependencies };
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

test "SQL missing qualified relations differ from unknown fields and internal metadata" {
    const columns = [_]Column{.{ .name = "id", .internal = "$id", .qualifier = "known", .type = .string, .nullable = true }};
    var builder: Builder = .{ .alloc = std.testing.allocator, .backend = undefined, .parameters = &.{} };
    try std.testing.expectError(error.UndefinedTable, builder.resolveField(&columns, "missing\x00id"));
    try std.testing.expectError(error.UndefinedColumn, builder.resolveField(&columns, "known\x00missing"));
    try std.testing.expectError(error.UndefinedColumn, builder.resolveField(&columns, "\x00metadata"));
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
    const virtual_columns = table.virtualColumns();
    const columns = try alloc.alloc(Column, table.columns.len + virtual_columns.len);
    for (table.columns, columns[0..table.columns.len]) |column, *out| out.* = .{ .name = column.name, .internal = column.name, .qualifier = name.table, .scope = physicalScope(table, name, aliased), .type = column.type, .element_type = column.element_type, .numeric_modifier = column.numeric_modifier, .temporal_kind = column.temporal_kind, .nullable = column.nullable };
    for (virtual_columns, columns[table.columns.len..]) |column, *out| out.* = .{ .name = column.name, .internal = column.name, .qualifier = name.table, .scope = physicalScope(table, name, aliased), .type = column.type, .element_type = column.element_type, .nullable = column.nullable, .visible = false };
    const source: Node = .{ .columns = columns, .operation = .singleton };
    return (try builder.lower(&source, .{ .table = name, .columns = projections })).columns;
}

/// Identity lineage through total, effect-free selection/projection frames.
/// Ordering, cardinality, aggregation and invocation boundaries are opaque.
pub const ScanColumn = struct { scan_index: usize, position: usize, name: []const u8 };
pub fn scanColumn(node_: *const Node, position_: usize) ?ScanColumn {
    var node = node_;
    var position = position_;
    for (0..32) |_| {
        if (position >= node.columns.len) return null;
        switch (node.operation) {
            .scan => |scan| return if (position < scan.source_columns.len) .{ .scan_index = scan.index, .position = position, .name = scan.source_columns[position] } else null,
            .query => |query| {
                if (query.statement.limit != null or query.statement.offset != null or query.statement.scalar_cardinality_limit or query.binding.aggregate != null or query.binding.window != null or query.binding.order_keys.len != 0 or query.constant_refs.len != 0) return null;
                // Every output must be safe, including columns beside the key.
                for (query.binding.scalars.projections) |program| if (program) |value| if (JoinPlanning.identity(value) == null) return null;
                if (query.binding.scalars.predicate) |predicate| if (!JoinPlanning.purePredicate(predicate)) return null;
                if (position >= query.statement.columns.len) return null;
                if (position < query.binding.scalars.projections.len and query.binding.scalars.projections[position] != null) {
                    position = JoinPlanning.identity(query.binding.scalars.projections[position].?) orelse return null;
                } else {
                    position = JoinPlanning.ordinal(query.source.columns, query.statement.columns[position].field) orelse return null;
                }
                node = query.source;
            },
            else => return null,
        }
    }
    return null;
}

/// Snapshot-local estimates guide bounded left-deep enumeration. Unknown
/// estimates decline reordering; estimates never become execution row limits.
pub const JoinPlanning = struct {
    pub const Estimate = struct {
        ptr: *anyopaque,
        rows: *const fn (*anyopaque, *const Node) ?u64,
        column: ?*const fn (*anyopaque, *const Node, usize) ?catalog.ColumnStatistics = null,
        bytes: ?*const fn (*anyopaque, *const Node) ?u64 = null,
        indexed: ?*const fn (*anyopaque, *const Node, usize) bool = null,
        memory_bytes: u64 = 64 * 1024 * 1024,
    };
    const Edge = struct {
        left: []const u8,
        right: []const u8,
        ltype: scalar.Type,
        rtype: scalar.Type,
        left_statistics: ?catalog.ColumnStatistics = null,
        right_statistics: ?catalog.ColumnStatistics = null,
        left_indexed: bool = false,
        right_indexed: bool = false,
    };
    leaves: [8]*const Node = undefined,
    count: usize = 0,
    edges: [64]Edge = undefined,
    edge_count: usize = 0,
    estimate: Estimate = undefined,
    cardinality: []const u64 = &.{},
    prepared: bool = false,
    leaf_widths: [8]f64 = @splat(0),
    fn ordinal(columns: []const Column, name: []const u8) ?usize {
        for (columns, 0..) |column, i| if (std.mem.eql(u8, column.internal, name)) return i;
        return null;
    }
    fn identity(program: scalar.Program) ?u32 {
        if (program.instructions.len != 1 or program.root != 0 or program.instructions[0].operation != .column) return null;
        return program.instructions[0].operation.column;
    }
    fn equalities(program: scalar.Program) ?usize {
        var count: usize = 0;
        for (program.instructions) |instruction| switch (instruction.operation) {
            .column => {},
            .binary => |binary| if (binary.op == .eq) {
                count += 1;
            } else if (binary.op != .@"and") return null,
            else => return null,
        };
        return count;
    }
    fn purePredicate(program: scalar.Program) bool {
        for (program.instructions) |instruction| switch (instruction.operation) {
            .column, .literal => {},
            .binary => |part| switch (part.op) {
                .eq, .neq, .lt, .lte, .gt, .gte, .@"and", .@"or" => {},
                else => return false,
            },
            else => return false,
        };
        return true;
    }
    fn collect(self: *JoinPlanning, node: *const Node) bool {
        if (node.operation == .join) {
            const join = node.operation.join;
            if (join.kind != .inner or join.correlation or join.membership != null or join.output_order != null or join.left_keys.len == 0 or join.left_keys.len != join.right_keys.len) return false;
            const condition = join.condition orelse return false;
            if ((equalities(condition) orelse return false) != join.left_keys.len) return false;
            if (!self.collect(join.left) or !self.collect(join.right)) return false;
            for (join.left_keys, join.right_keys) |left, right| {
                if (self.edge_count == self.edges.len) return false;
                const l = identity(left) orelse return false;
                const r = identity(right) orelse return false;
                if (l >= join.left.columns.len or r >= join.right.columns.len) return false;
                self.edges[self.edge_count] = .{ .left = join.left.columns[l].internal, .right = join.right.columns[r].internal, .ltype = left.output_type, .rtype = right.output_type };
                self.edge_count += 1;
            }
            return true;
        }
        // Independent scan/filter producers preserve their own evaluation
        // boundary. Lateral, recursive, materialized and outer joins decline.
        if (node.operation != .scan) {
            if (node.operation != .query or node.operation.query.source.operation != .scan) return false;
            const query = node.operation.query;
            if (query.statement.limit != null or query.statement.offset != null or query.binding.aggregate != null or query.binding.window != null or query.binding.order_keys.len != 0) return false;
            for (query.binding.scalars.projections) |program| if (program) |value| {
                if (identity(value) == null) return false;
            };
            if (query.binding.scalars.predicate) |predicate| for (predicate.instructions) |instruction| switch (instruction.operation) {
                .column, .literal => {},
                .binary => |binary| switch (binary.op) {
                    .eq, .neq, .lt, .lte, .gt, .gte, .@"and", .@"or" => {},
                    else => return false,
                },
                else => return false,
            };
        }
        if (self.count == self.leaves.len) return false;
        self.leaves[self.count] = node;
        self.count += 1;
        return true;
    }
    fn leaf(self: *const JoinPlanning, name: []const u8) usize {
        for (self.leaves[0..self.count], 0..) |node, index| if (ordinal(node.columns, name) != null) return index;
        unreachable;
    }
    fn crossing(self: *const JoinPlanning, mask: usize, next: usize) usize {
        var count: usize = 0;
        for (self.edges[0..self.edge_count]) |edge| {
            const l = self.leaf(edge.left);
            const r = self.leaf(edge.right);
            if ((l == next and mask & (@as(usize, 1) << @intCast(r)) != 0) or (r == next and mask & (@as(usize, 1) << @intCast(l)) != 0)) count += 1;
        }
        return count;
    }
    fn statistics(self: *const JoinPlanning, name: []const u8) ?catalog.ColumnStatistics {
        if (self.prepared) {
            for (self.edges[0..self.edge_count]) |edge| {
                if (std.mem.eql(u8, edge.left, name)) return edge.left_statistics;
                if (std.mem.eql(u8, edge.right, name)) return edge.right_statistics;
            }
            return null;
        }
        const callback = self.estimate.column orelse return null;
        const index = self.leaf(name);
        return callback(self.estimate.ptr, self.leaves[index], ordinal(self.leaves[index].columns, name).?);
    }
    fn number(rows: f64) u64 {
        return @intFromFloat(@min(@as(f64, @floatFromInt(std.math.maxInt(u64) - 2048)), @max(0, @ceil(rows))));
    }
    fn joinedRows(self: *const JoinPlanning, left_mask: usize, right_mask: usize, left_rows: u64, right_rows: u64) u64 {
        var result: ?u64 = null;
        for (self.edges[0..self.edge_count]) |edge_| {
            var edge = edge_;
            if (left_mask & (@as(usize, 1) << @intCast(self.leaf(edge.left))) == 0 or right_mask & (@as(usize, 1) << @intCast(self.leaf(edge.right))) == 0) {
                std.mem.swap([]const u8, &edge.left, &edge.right);
                if (left_mask & (@as(usize, 1) << @intCast(self.leaf(edge.left))) == 0 or right_mask & (@as(usize, 1) << @intCast(self.leaf(edge.right))) == 0) continue;
            }
            const l = self.statistics(edge.left) orelse continue;
            const r = self.statistics(edge.right) orelse continue;
            if (l.min_integer != null and l.max_integer != null and r.min_integer != null and r.max_integer != null and (l.min_integer.? > r.max_integer.? or r.min_integer.? > l.max_integer.?)) return 0;
            const lbase = l.population_rows orelse self.cardinality[self.leaf(edge.left)];
            const rbase = r.population_rows orelse self.cardinality[self.leaf(edge.right)];
            const lrows = @as(f64, @floatFromInt(left_rows)) * (if (l.non_null_rows) |n| @min(1.0, @as(f64, @floatFromInt(n)) / @as(f64, @floatFromInt(@max(1, lbase)))) else 1.0);
            const rrows = @as(f64, @floatFromInt(right_rows)) * (if (r.non_null_rows) |n| @min(1.0, @as(f64, @floatFromInt(n)) / @as(f64, @floatFromInt(@max(1, rbase)))) else 1.0);
            const ldistinct: f64 = if (l.unique) lrows else if (l.distinct_count) |n| @floatFromInt(@min(left_rows, n)) else continue;
            const rdistinct: f64 = if (r.unique) rrows else if (r.distinct_count) |n| @floatFromInt(@min(right_rows, n)) else continue;
            var hot: f64 = 0;
            var lhot: f64 = 0;
            var rhot: f64 = 0;
            var matched: f64 = 0;
            // Providers cap this list; cap again at the planner boundary.
            for (l.frequencies[0..@min(32, l.frequencies.len)]) |lf| for (r.frequencies[0..@min(32, r.frequencies.len)]) |rf| {
                if (lf.hash != rf.hash) continue;
                const lc = @min(lrows, @as(f64, @floatFromInt(lf.rows)) * lrows / @as(f64, @floatFromInt(@max(1, lbase))));
                const rc = @min(rrows, @as(f64, @floatFromInt(rf.rows)) * rrows / @as(f64, @floatFromInt(@max(1, rbase))));
                lhot += lc;
                rhot += rc;
                hot += lc * rc;
                matched += 1;
                break;
            };
            const rows = number(hot + @max(0, lrows - lhot) * @max(0, rrows - rhot) / @max(1, @max(ldistinct, rdistinct) - matched));
            result = if (result) |old| @min(old, rows) else rows;
        }
        return result orelse @min(left_rows, right_rows);
    }
    fn width(self: *const JoinPlanning, mask: usize) f64 {
        var bytes: f64 = 0;
        for (self.leaves[0..self.count], 0..) |node, i| if (mask & (@as(usize, 1) << @intCast(i)) != 0) {
            if (self.prepared) {
                bytes += self.leaf_widths[i];
                continue;
            }
            const known = if (self.estimate.bytes) |callback| callback(self.estimate.ptr, node) else null;
            bytes += if (known) |n| @max(8.0, @as(f64, @floatFromInt(n)) / @as(f64, @floatFromInt(@max(1, self.cardinality[i])))) else @as(f64, @floatFromInt(@max(16, node.columns.len * 16)));
        };
        return bytes;
    }
    fn transitionCost(self: *const JoinPlanning, lmask: usize, rmask: usize, lrows: u64, rrows: u64, output: u64) f64 {
        const lb = @as(f64, @floatFromInt(lrows)) * self.width(lmask);
        const rb = @as(f64, @floatFromInt(rrows)) * self.width(rmask);
        var cost = lb + rb + @as(f64, @floatFromInt(output)) * self.width(lmask | rmask);
        // Account for hash metadata and serialized state; crossing the memory
        // grant adds write/read traffic instead of treating spill as free.
        const build_bytes = @min(lb + @as(f64, @floatFromInt(lrows)) * 32, rb + @as(f64, @floatFromInt(rrows)) * 32);
        if (build_bytes > @as(f64, @floatFromInt(self.estimate.memory_bytes))) cost += build_bytes * 2;
        for (self.edges[0..self.edge_count]) |edge| {
            const l = self.leaf(edge.left);
            const r = self.leaf(edge.right);
            if (lmask & (@as(usize, 1) << @intCast(l)) != 0 and rmask == @as(usize, 1) << @intCast(r) and edge.right_indexed) cost = @min(cost, lb + @as(f64, @floatFromInt(lrows)) * 64 + @as(f64, @floatFromInt(output)) * self.width(lmask | rmask));
            if (rmask & (@as(usize, 1) << @intCast(r)) != 0 and lmask == @as(usize, 1) << @intCast(l) and edge.left_indexed) cost = @min(cost, rb + @as(f64, @floatFromInt(rrows)) * 64 + @as(f64, @floatFromInt(output)) * self.width(lmask | rmask));
        }
        return cost;
    }
    fn originalCost(self: *const JoinPlanning, node: *const Node, estimates: []const u64) struct { rows: u64, cost: f64, mask: usize } {
        if (node.operation == .join) {
            const left = self.originalCost(node.operation.join.left, estimates);
            const right = self.originalCost(node.operation.join.right, estimates);
            const rows = self.joinedRows(left.mask, right.mask, left.rows, right.rows);
            return .{ .rows = rows, .cost = left.cost + right.cost + self.transitionCost(left.mask, right.mask, left.rows, right.rows, rows), .mask = left.mask | right.mask };
        }
        for (self.leaves[0..self.count], estimates, 0..) |leaf_, rows, i| if (leaf_ == node) return .{ .rows = rows, .cost = 0, .mask = @as(usize, 1) << @intCast(i) };
        unreachable;
    }
    fn key(a: Allocator, template: scalar.Program, position: usize, type_: scalar.Type) !scalar.Program {
        var program = template;
        program.instructions = try a.dupe(scalar.Instruction, &.{.{ .type = type_, .operation = .{ .column = @intCast(position) } }});
        program.required_columns = try a.dupe(u32, &.{@intCast(position)});
        program.root = 0;
        program.output_type = type_;
        return program;
    }
    fn build(self: *JoinPlanning, a: Allocator, original: *const Node, order: []const usize, estimates: []const u64) !*const Node {
        const template = original.operation.join.condition.?;
        var left = self.leaves[order[0]];
        var rows = estimates[order[0]];
        var mask = @as(usize, 1) << @intCast(order[0]);
        for (order[1..], 1..) |next, stage| {
            const right = self.leaves[next];
            const columns = try a.alloc(Column, left.columns.len + right.columns.len);
            @memcpy(columns[0..left.columns.len], left.columns);
            @memcpy(columns[left.columns.len..], right.columns);
            const final = stage + 1 == order.len;
            const logical = if (final) original.columns else columns;
            var instructions: std.ArrayList(scalar.Instruction) = .empty;
            var required: std.ArrayList(u32) = .empty;
            var left_keys: std.ArrayList(scalar.Program) = .empty;
            var right_keys: std.ArrayList(scalar.Program) = .empty;
            var root: ?u32 = null;
            for (self.edges[0..self.edge_count]) |edge_| {
                var edge = edge_;
                if (ordinal(left.columns, edge.left) == null or ordinal(right.columns, edge.right) == null) {
                    if (ordinal(left.columns, edge.right) == null or ordinal(right.columns, edge.left) == null) continue;
                    std.mem.swap([]const u8, &edge.left, &edge.right);
                    std.mem.swap(scalar.Type, &edge.ltype, &edge.rtype);
                }
                try left_keys.append(a, try key(a, template, ordinal(left.columns, edge.left).?, edge.ltype));
                try right_keys.append(a, try key(a, template, ordinal(right.columns, edge.right).?, edge.rtype));
                const start: u32 = @intCast(instructions.items.len);
                const l: u32 = @intCast(ordinal(logical, edge.left).?);
                const r: u32 = @intCast(ordinal(logical, edge.right).?);
                try instructions.appendSlice(a, &.{
                    .{ .type = edge.ltype, .operation = .{ .column = l } },
                    .{ .type = edge.rtype, .operation = .{ .column = r } },
                    .{ .type = .{ .kind = .boolean }, .operation = .{ .binary = .{ .op = .eq, .left = start, .right = start + 1 } } },
                });
                try required.appendSlice(a, &.{ l, r });
                if (root) |previous| try instructions.append(a, .{ .type = .{ .kind = .boolean }, .operation = .{ .binary = .{ .op = .@"and", .left = previous, .right = start + 2 } } });
                root = @intCast(instructions.items.len - 1);
            }
            var condition = template;
            condition.instructions = try instructions.toOwnedSlice(a);
            condition.required_columns = try required.toOwnedSlice(a);
            condition.root = root orelse return error.InvalidSqlBackendResponse;
            condition.output_type = .{ .kind = .boolean };
            var permutation: ?[]usize = null;
            if (final) {
                permutation = try a.alloc(usize, logical.len);
                for (logical, permutation.?) |column, *position| position.* = ordinal(columns, column.internal).?;
            }
            const node = try a.create(Node);
            const bit = @as(usize, 1) << @intCast(next);
            rows = self.joinedRows(mask, bit, rows, estimates[next]);
            mask |= bit;
            node.* = .{ .columns = logical, .operation = .{ .join = .{ .kind = .inner, .left = left, .right = right, .condition = condition, .left_keys = try left_keys.toOwnedSlice(a), .right_keys = try right_keys.toOwnedSlice(a), .output_order = permutation, .estimated_rows = rows } } };
            left = node;
        }
        return left;
    }
    fn selectivity(program: scalar.Program, index: u32, depth: usize) f64 {
        if (depth == 32) return 0.5;
        return switch (program.instructions[index].operation) {
            .literal => |literal| if (literal == .bool) (if (literal.bool) @as(f64, 1) else 0) else if (literal == .null) 0 else 0.5,
            .binary => |part| switch (part.op) {
                .@"and" => selectivity(program, part.left, depth + 1) * selectivity(program, part.right, depth + 1),
                .@"or" => union_: {
                    const left = selectivity(program, part.left, depth + 1);
                    const right = selectivity(program, part.right, depth + 1);
                    break :union_ left + right - left * right;
                },
                .eq => 1.0 / 16.0,
                .neq => 15.0 / 16.0,
                .lt, .lte, .gt, .gte => 0.25,
                else => 0.5,
            },
            else => 0.5,
        };
    }
    pub fn filteredRows(rows: u64, program: scalar.Program) u64 {
        const fraction = selectivity(program, program.root, 0);
        if (fraction == 0 or rows == 0) return 0;
        const estimate = @min(@as(f64, @floatFromInt(std.math.maxInt(u64) - 2048)), @ceil(@as(f64, @floatFromInt(rows)) * fraction));
        return @max(1, @as(u64, @intFromFloat(estimate)));
    }
    pub fn optimize(a: Allocator, original: *const Node, estimate: Estimate) anyerror!*const Node {
        if (original.operation == .query) {
            const child = try optimize(a, original.operation.query.source, estimate);
            if (child == original.operation.query.source) return original;
            const node = try a.create(Node);
            node.* = original.*;
            node.operation.query.source = child;
            return node;
        }
        var plan: JoinPlanning = .{ .estimate = estimate };
        if (!plan.collect(original) or plan.count < 3) return original;
        var cardinality: [8]u64 = undefined;
        for (plan.leaves[0..plan.count], cardinality[0..plan.count]) |leaf_, *rows| rows.* = estimate.rows(estimate.ptr, leaf_) orelse return original;
        plan.cardinality = cardinality[0..plan.count];
        // Negotiate immutable evidence once, outside the DP's subset loops.
        for (0..plan.count) |i| plan.leaf_widths[i] = plan.width(@as(usize, 1) << @intCast(i));
        for (plan.edges[0..plan.edge_count]) |*edge| {
            edge.left_statistics = plan.statistics(edge.left);
            edge.right_statistics = plan.statistics(edge.right);
            if (estimate.indexed) |callback| {
                const l = plan.leaf(edge.left);
                const r = plan.leaf(edge.right);
                edge.left_indexed = callback(estimate.ptr, plan.leaves[l], ordinal(plan.leaves[l].columns, edge.left).?);
                edge.right_indexed = callback(estimate.ptr, plan.leaves[r], ordinal(plan.leaves[r].columns, edge.right).?);
            }
        }
        plan.prepared = true;
        var costs: [256]f64 = @splat(std.math.inf(f64));
        var sizes: [256]u64 = @splat(0);
        var last: [256]u8 = undefined;
        for (cardinality[0..plan.count], 0..) |rows, leaf_| {
            const mask = @as(usize, 1) << @intCast(leaf_);
            costs[mask] = 0;
            sizes[mask] = rows;
            last[mask] = @intCast(leaf_);
        }
        const full = (@as(usize, 1) << @intCast(plan.count)) - 1;
        for (1..full) |mask| {
            if (!std.math.isFinite(costs[mask])) continue;
            for (0..plan.count) |next| {
                const bit = @as(usize, 1) << @intCast(next);
                if (mask & bit != 0 or plan.crossing(mask, next) == 0) continue;
                const rows = plan.joinedRows(mask, bit, sizes[mask], cardinality[next]);
                const cost = costs[mask] + plan.transitionCost(mask, bit, sizes[mask], cardinality[next], rows);
                if (cost < costs[mask | bit]) {
                    costs[mask | bit] = cost;
                    sizes[mask | bit] = rows;
                    last[mask | bit] = @intCast(next);
                }
            }
        }
        if (!std.math.isFinite(costs[full])) return original;
        if (costs[full] >= plan.originalCost(original, cardinality[0..plan.count]).cost) return original;
        var order: [8]usize = undefined;
        var mask = full;
        var remaining = plan.count;
        var changed = false;
        while (remaining != 0) {
            remaining -= 1;
            order[remaining] = last[mask];
            changed = changed or order[remaining] != remaining;
            mask ^= @as(usize, 1) << @intCast(last[mask]);
        }
        return if (changed) plan.build(a, original, order[0..plan.count], cardinality[0..plan.count]) else original;
    }
};

test "SQL snapshot join planning reduces connected intermediates and preserves logical output coordinates" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Helper = struct {
        fn program(alloc: Allocator, text: []const u8, names: []const []const u8) !scalar.Program {
            var compiled = try compiler.compileScalar(alloc, text, .{});
            defer compiled.deinit();
            const columns = try alloc.alloc(scalar.Column, names.len);
            for (columns, names) |*column, name| column.* = .{ .name = name, .type = .integer };
            return scalar.bind(alloc, compiled.expression, columns, &.{}, .{});
        }
        fn rows(_: *anyopaque, node: *const Node) ?u64 {
            return if (node.operation == .scan) (if (node.operation.scan.index == 2) @as(u64, 1) else 1_000_000) else null;
        }
        fn skewRows(_: *anyopaque, node: *const Node) ?u64 {
            return if (node.operation.scan.index == 2) 100000 else 1000;
        }
        fn statistics(_: *anyopaque, node: *const Node, _: usize) ?catalog.ColumnStatistics {
            return .{ .name = node.columns[0].name, .distinct_count = if (node.operation.scan.index == 2) 100000 else 1 };
        }
    };
    var leaves: [3]Node = undefined;
    const names = [_][]const u8{ "a", "b", "c" };
    const columns = try a.alloc(Column, 3);
    for (&leaves, names, columns, 0..) |*leaf_, name, *column, index| {
        column.* = .{ .name = name, .internal = name, .qualifier = name, .type = .integer, .nullable = true };
        leaf_.* = .{ .columns = columns[index..][0..1], .operation = .{ .scan = .{ .index = index, .source_columns = try a.dupe([]const u8, &.{name}) } } };
    }
    const ab: Node = .{ .columns = columns[0..2], .operation = .{ .join = .{ .kind = .inner, .left = &leaves[0], .right = &leaves[1], .condition = try Helper.program(a, "a=b", names[0..2]), .left_keys = try a.dupe(scalar.Program, &.{try Helper.program(a, "a", &.{"a"})}), .right_keys = try a.dupe(scalar.Program, &.{try Helper.program(a, "b", &.{"b"})}) } } };
    const abc: Node = .{ .columns = columns, .operation = .{ .join = .{ .kind = .inner, .left = &ab, .right = &leaves[2], .condition = try Helper.program(a, "b=c", &names), .left_keys = try a.dupe(scalar.Program, &.{try Helper.program(a, "b", names[0..2])}), .right_keys = try a.dupe(scalar.Program, &.{try Helper.program(a, "c", &.{"c"})}) } } };
    var context: u8 = 0;
    const optimized = try JoinPlanning.optimize(a, &abc, .{ .ptr = &context, .rows = Helper.rows });
    try std.testing.expect(optimized != &abc);
    try std.testing.expectEqualSlices(Column, columns, optimized.columns);
    try std.testing.expect(optimized.operation.join.output_order != null);
    const first = optimized.operation.join.left.operation.join;
    try std.testing.expect(first.left == &leaves[2] or first.right == &leaves[2]);
    try std.testing.expectEqual(@as(?u64, 1), optimized.operation.join.estimated_rows);
    // A/B both contain one hot key. Joining the larger distinct C relation
    // first avoids constructing the million-row A/B intermediate.
    const skewed = try JoinPlanning.optimize(a, &abc, .{ .ptr = &context, .rows = Helper.skewRows, .column = Helper.statistics });
    try std.testing.expect(skewed != &abc);
    const skew_first = skewed.operation.join.left.operation.join;
    try std.testing.expect(skew_first.left == &leaves[2] or skew_first.right == &leaves[2]);
    try std.testing.expectEqual(@as(?u64, 1000000), skewed.operation.join.estimated_rows);
    var outer = abc;
    outer.operation.join.kind = .left;
    try std.testing.expectEqual(&outer, try JoinPlanning.optimize(a, &outer, .{ .ptr = &context, .rows = Helper.rows }));
    const conjunction = try Helper.program(a, "a=1 AND a=2", &.{"a"});
    const disjunction = try Helper.program(a, "a=1 OR a=2", &.{"a"});
    try std.testing.expect(JoinPlanning.filteredRows(1024, conjunction) < JoinPlanning.filteredRows(1024, disjunction));
}

test "SQL join costing accounts for heavy keys nulls unique domains widths and spill" {
    const Fixture = struct {
        fn rows(_: *anyopaque, _: *const Node) ?u64 {
            return 10000;
        }
        fn column(raw: *anyopaque, node: *const Node, _: usize) ?catalog.ColumnStatistics {
            const statistics: *[2]catalog.ColumnStatistics = @ptrCast(@alignCast(raw));
            return statistics[node.operation.scan.index];
        }
        fn bytes(_: *anyopaque, _: *const Node) ?u64 {
            return 10000 * 1024;
        }
    };
    const lcol = [_]Column{.{ .name = "l", .internal = "l", .qualifier = "l", .type = .integer, .nullable = true }};
    const rcol = [_]Column{.{ .name = "r", .internal = "r", .qualifier = "r", .type = .integer, .nullable = true }};
    const left: Node = .{ .columns = &lcol, .operation = .{ .scan = .{ .index = 0, .source_columns = &.{"l"} } } };
    const right: Node = .{ .columns = &rcol, .operation = .{ .scan = .{ .index = 1, .source_columns = &.{"r"} } } };
    var statistics = [_]catalog.ColumnStatistics{ .{ .name = "l", .distinct_count = 100 }, .{ .name = "r", .distinct_count = 100 } };
    var plan: JoinPlanning = .{ .count = 2, .edge_count = 1, .cardinality = &.{ 10000, 10000 }, .estimate = .{ .ptr = &statistics, .rows = Fixture.rows, .column = Fixture.column, .bytes = Fixture.bytes } };
    plan.leaves[0..2].* = .{ &left, &right };
    plan.edges[0] = .{ .left = "l", .right = "r", .ltype = .{ .kind = .integer }, .rtype = .{ .kind = .integer } };
    try std.testing.expectEqual(@as(u64, 1000000), plan.joinedRows(1, 2, 10000, 10000));
    statistics[0].frequencies = &.{.{ .hash = 7, .rows = 5000 }};
    statistics[1].frequencies = &.{.{ .hash = 7, .rows = 5000 }};
    try std.testing.expect(plan.joinedRows(1, 2, 10000, 10000) > 25000000);
    statistics[0].frequencies = &.{};
    statistics[1].frequencies = &.{};
    statistics[0].unique = true;
    try std.testing.expectEqual(@as(u64, 10000), plan.joinedRows(1, 2, 10000, 10000));
    statistics[0].non_null_rows = 5000;
    try std.testing.expectEqual(@as(u64, 10000), plan.joinedRows(1, 2, 10000, 10000));
    statistics[0].min_integer = 1;
    statistics[0].max_integer = 4;
    statistics[1].min_integer = 5;
    statistics[1].max_integer = 9;
    try std.testing.expectEqual(@as(u64, 0), plan.joinedRows(1, 2, 10000, 10000));
    const resident = plan.transitionCost(1, 2, 10000, 10000, 100);
    plan.estimate.memory_bytes = 4096;
    try std.testing.expect(plan.transitionCost(1, 2, 10000, 10000, 100) > resident);
}
