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

//! Non-executing semantic binding shared by Describe and Execute. Resolving a
//! catalog definition authorizes access but never opens a row reader or stages
//! a mutation. Parameter inference is structural, not dummy-value execution.
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const compiler = @import("compiler.zig");
const bound_scalars = @import("bound_scalars.zig");
const Json = std.json.Value;

test "SQL DDL description has no fabricated table or mutation side effects" {
    const Backend = struct {
        fn checkpoint(_: *anyopaque) !void {}
        fn ddl(_: *anyopaque, _: std.mem.Allocator, _: catalog.Ddl) !catalog.DdlOutcome {
            return error.UnexpectedMutation;
        }
    };
    var compiled = try compiler.compile(std.testing.allocator, "CREATE TABLE t (id BIGINT)", .{});
    defer compiled.deinit();
    var description = try describe(std.testing.allocator, .{ .ptr = undefined, .vtable = &.{ .resolve = undefined, .scan = undefined, .mutate = undefined, .checkpoint = Backend.checkpoint, .ddl = Backend.ddl } }, &compiled, &.{});
    defer description.deinit();
    try std.testing.expect(description.binding.table == null);
    try std.testing.expectEqual(@as(usize, 0), description.binding.columns.len);
    try std.testing.expectEqual(@as(usize, 0), description.binding.parameter_types.len);
}

pub const Column = struct {
    name: []const u8,
    type: ast.ColumnType,
    element_type: ?@import("array_value.zig").ElementType = null,
    /// NULL without a concrete SQL type can adopt an assignment/set context.
    /// It must not be confused with a typed string expression that is NULL.
    untyped_null: bool = false,

    pub fn jsonStringify(self: Column, writer: anytype) !void {
        if (self.type == .array) {
            try writer.write(.{ .name = self.name, .type = self.type, .element_type = self.element_type });
        } else try writer.write(.{ .name = self.name, .type = self.type });
    }
};

fn internalKind(kind: ?ast.ColumnType) ast.ColumnType {
    return kind orelse .string;
}

test "SQL column JSON excludes internal unknown NULL provenance" {
    const encoded = try std.json.Stringify.valueAlloc(std.testing.allocator, Column{ .name = "value", .type = .integer, .untyped_null = true }, .{});
    defer std.testing.allocator.free(encoded);
    try std.testing.expectEqualStrings("{\"name\":\"value\",\"type\":\"integer\"}", encoded);
    const array = try std.json.Stringify.valueAlloc(std.testing.allocator, Column{ .name = "items", .type = .array, .element_type = .int32 }, .{});
    defer std.testing.allocator.free(array);
    try std.testing.expectEqualStrings("{\"name\":\"items\",\"type\":\"array\",\"element_type\":\"int32\"}", array);
}
pub const OrderKey = struct {
    source: union(enum) { output: usize, column: catalog.Column, expression: usize },
    descending: bool,
    nulls_first: ?bool = null,
};
pub const BoundStatement = struct {
    parameter_invocation: ?*@import("parameter_binding.zig").Invocation = null,
    parameter_descriptors: []const @import("scalar.zig").Type = &.{},
    joined_mutation: ?*const @import("joined_mutation.zig").Bound = null,
    merge_mutation: ?*const @import("merge_mutation.zig").Candidates = null,
    conflict: ?@import("conflict.zig").Bound = null,
    insert_source: ?*const BoundStatement = null,
    returning: ?*const BoundStatement = null,
    returning_projections: ?[]const ast.Projection = null,
    returning_query: ?ast.Select = null,
    relation: ?*const @import("relation_binding.zig").Bound = null,
    aggregate: ?*const @import("aggregate_binding.zig").Bound = null,
    window: ?*const @import("window_binding.zig").Bound = null,
    scalars: bound_scalars.Bound = .{},
    order_keys: []const OrderKey = &.{},
    primary_order: bool = false,
    /// Physical table identity and schema version belong to this binding,
    /// never the reusable schema-independent Compiled statement. Rebinding
    /// after catalog publication is mandatory before a later execution.
    table: ?catalog.Table,
    action: catalog.Action,
    columns: []const Column,
    /// One entry per positional slot, including unused holes. Null means the
    /// context and optional explicit hints provide no type for that slot.
    parameter_types: []const ?ast.ColumnType,
    /// Parsed typed JSON literals, deduplicated by their SQL string contents.
    /// Keys and complete trees belong to this binding's allocator. Execute
    /// reuses these immutable values instead of parsing literals a second time.
    json_literals: std.StringHashMapUnmanaged(Json),
};

pub const Description = struct {
    arena: std.heap.ArenaAllocator,
    binding: BoundStatement,
    settings: ?*@import("setting_catalog.zig").View = null,

    pub fn deinit(self: *Description) void {
        if (self.settings) |view| view.deinit();
        self.arena.deinit();
        self.* = undefined;
    }
};

pub fn describe(allocator: std.mem.Allocator, backend: catalog.Backend, compiled: *const compiler.Compiled, explicit_parameter_types: []const ?ast.ColumnType) !Description {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    var statement_backend = backend;
    var settings: ?*@import("setting_catalog.zig").View = null;
    if (backend.setting_capture) |capture| {
        const view = try arena.allocator().create(@import("setting_catalog.zig").View);
        view.* = try @import("setting_catalog.zig").View.capture(allocator, capture.owner, capture.scope, capture.overlay);
        settings = view;
        statement_backend.settings_view = view;
    }
    errdefer if (settings) |view| view.deinit();
    const binding = try bind(arena.allocator(), statement_backend, compiled, explicit_parameter_types);
    return .{ .arena = arena, .binding = binding, .settings = settings };
}

fn assignmentLiteral(value: ast.Value, kind: ast.ColumnType) !ast.Value {
    if (value == .parameter or value == .null) return value;
    const raw: std.json.Value = switch (value) {
        .integer => |v| .{ .integer = v },
        .number => |v| .{ .float = v },
        .boolean => |v| .{ .bool = v },
        .string => |v| .{ .string = v },
        .null, .parameter => unreachable,
    };
    return switch (try coerce(raw, kind)) {
        .integer => |v| .{ .integer = v },
        .float => |v| .{ .number = v },
        .bool => |v| .{ .boolean = v },
        .string => |v| .{ .string = v },
        .null => .null,
        else => error.SqlTypeMismatch,
    };
}

fn typedValuesSource(allocator: std.mem.Allocator, source: *const ast.Select, insertion: ast.Insert, table: catalog.Table, row_index: *usize) anyerror!*const ast.Select {
    const result = try allocator.create(ast.Select);
    result.* = source.*;
    if (source.values_arms.len != 0) {
        const arms = try allocator.alloc(*const ast.Select, source.values_arms.len);
        for (source.values_arms, arms) |arm, *out| out.* = try typedValuesSource(allocator, arm, insertion, table, row_index);
        result.values_arms = arms;
        return result;
    }
    if (source.set_operation) |set| {
        result.set_operation = .{
            .kind = set.kind,
            .all = set.all,
            .left = try typedValuesSource(allocator, set.left, insertion, table, row_index),
            .right = try typedValuesSource(allocator, set.right, insertion, table, row_index),
        };
        return result;
    }
    if (row_index.* >= insertion.values_source_rows.len or source.columns.len != insertion.columns.len + (if (insertion.conflict) |clause| clause.capture_count else @as(usize, 0))) return error.InvalidSqlParameters;
    const row = insertion.values_source_rows[row_index.*];
    const default_row = row_index.*;
    row_index.* += 1;
    if (row.len != insertion.columns.len) return error.InvalidSqlParameters;
    const projections = try allocator.dupe(ast.Projection, source.columns);
    for (projections[0..insertion.columns.len], row, insertion.columns, 0..) |*projection, original, name, cell_index| {
        if (insertion.isDefault(default_row, cell_index)) continue;
        const expression = projection.expression orelse return error.InvalidSqlBackendResponse;
        if (expression.* != .literal or original == .parameter) continue;
        const literal = try allocator.create(ast.Scalar);
        literal.* = .{ .literal = try assignmentLiteral(original, (try table.column(name)).type) };
        projection.expression = literal;
    }
    result.columns = projections;
    return result;
}

/// All returned data belongs to allocator. Callers must supply a bounded
/// request/description arena and discard that arena on binding failure.
/// The backend's definition lookup occurs once;
/// execution reuses binding.table instead of resolving another schema epoch.
pub fn bind(allocator: std.mem.Allocator, backend: catalog.Backend, compiled: *const compiler.Compiled, explicit_parameter_types: []const ?ast.ColumnType) anyerror!BoundStatement {
    const result = try bindInternal(allocator, backend, compiled, explicit_parameter_types);
    for (result.columns) |column| if (column.type == .array and column.element_type == null) return error.SqlTypeMismatch;
    return result;
}

/// Internal plans retain full typed descriptors. Wire validation belongs only
/// at the public statement boundary, never at a derived relation boundary.
pub fn bindInternal(allocator: std.mem.Allocator, backend: catalog.Backend, compiled: *const compiler.Compiled, explicit_parameter_types: []const ?ast.ColumnType) anyerror!BoundStatement {
    var statement_backend = backend;
    if (statement_backend.parameter_invocation == null and compiled.parameter_count != 0) {
        const invocation = try @import("parameter_binding.zig").Invocation.initLeaky(allocator, compiled.parameter_count);
        invocation.fallbacks = backend.parameter_fallback_types;
        try invocation.mergeInferred(backend.parameter_descriptor_hints);
        statement_backend.parameter_invocation = invocation;
    }
    if (statement_backend.parameter_invocation) |invocation| try invocation.mergeCoarse(explicit_parameter_types);
    var result = try bindImpl(allocator, statement_backend, compiled, explicit_parameter_types);
    result.parameter_invocation = statement_backend.parameter_invocation;
    if (statement_backend.parameter_invocation) |invocation| {
        try invocation.mergeCoarse(result.parameter_types);
        result.parameter_descriptors = invocation.descriptors;
    }
    return result;
}

fn bindImpl(allocator: std.mem.Allocator, backend: catalog.Backend, compiled: *const compiler.Compiled, explicit_parameter_types: []const ?ast.ColumnType) anyerror!BoundStatement {
    if (compiled.statement == .explain) {
        const inner: compiler.Compiled = .{ .arena = undefined, .statement = compiled.statement.explain.statement.*, .parameter_count = compiled.parameter_count };
        var inspected = try bindInternal(allocator, backend, &inner, explicit_parameter_types);
        // Keep the complete inner identity manifest for prepared/pgwire
        // validation. An EXPLAIN of a join must not pin only its first table.
        inspected.action = .read;
        inspected.columns = try allocator.dupe(Column, &.{.{ .name = "QUERY PLAN", .type = .string }});
        return inspected;
    }
    if (explicit_parameter_types.len > compiled.parameter_count) return error.InvalidSqlParameters;
    if (compiled.statement == .select) try @import("window_binding.zig").validatePlacement(compiled.statement.select);
    if (compiled.statement == .select and @import("subquery_lowering.zig").accepts(compiled.statement.select)) {
        var lowered = compiled.*;
        lowered.statement = .{ .select = try @import("subquery_lowering.zig").lower(allocator, compiled.statement.select) };
        return bindInternal(allocator, backend, &lowered, explicit_parameter_types);
    }
    // A mutation scalar subquery needs the same captured, decorrelated source
    // plan as joined DML. Route target-only UPDATE/DELETE through that planner
    // so the inner relation is scanned once under the statement snapshot,
    // never once per target row or outside the mutation's range proof.
    if (compiled.statement == .update and compiled.statement.update.source == null) {
        const lowering = @import("subquery_lowering.zig");
        var mutation = compiled.statement.update;
        var needs_relation = if (mutation.predicate) |predicate| lowering.predicateHas(predicate) else false;
        needs_relation = needs_relation or returningReads(mutation.returning);
        for (mutation.assignments) |assignment| {
            needs_relation = needs_relation or assignment.use_default;
            if (assignment.expression) |expression| needs_relation = needs_relation or lowering.has(expression);
        }
        if (needs_relation) {
            const source = try allocator.create(ast.Relation);
            source.* = .{ .table = .{ .name = mutation.table, .alias = mutation.alias, .mutation_target = true, .mutation_document = true, .mutation_presence = true } };
            mutation.source = source;
            var lowered = compiled.*;
            lowered.statement = .{ .update = mutation };
            return bindInternal(allocator, backend, &lowered, explicit_parameter_types);
        }
    }
    if (compiled.statement == .delete and compiled.statement.delete.source == null) {
        const lowering = @import("subquery_lowering.zig");
        var mutation = compiled.statement.delete;
        if ((if (mutation.predicate) |predicate| lowering.predicateHas(predicate) else false) or returningReads(mutation.returning)) {
            const source = try allocator.create(ast.Relation);
            source.* = .{ .table = .{ .name = mutation.table, .alias = mutation.alias, .mutation_target = true, .mutation_presence = true } };
            mutation.source = source;
            var lowered = compiled.*;
            lowered.statement = .{ .delete = mutation };
            return bindInternal(allocator, backend, &lowered, explicit_parameter_types);
        }
    }
    if (compiled.statement == .select and @import("relation_binding.zig").accepts(compiled.statement.select)) {
        const relations = @import("relation_binding.zig");
        const parameters = try allocator.alloc(?ast.ColumnType, compiled.parameter_count);
        @memset(parameters, null);
        @memcpy(parameters[0..explicit_parameter_types.len], explicit_parameter_types);
        const relation = try allocator.create(relations.Bound);
        relation.* = try relations.bind(allocator, backend, compiled.statement.select, parameters);
        var adapter: relations.ResolveAdapter = .{ .backend = backend, .table = relation.table };
        const lowered: compiler.Compiled = .{ .arena = undefined, .statement = .{ .select = relation.statement }, .parameter_count = compiled.parameter_count };
        var result = try bindInternal(allocator, adapter.iface(), &lowered, parameters);
        result.relation = relation;
        const output_columns = try allocator.dupe(Column, result.columns);
        for (output_columns, 0..) |*column, index| column.untyped_null = column.untyped_null or relations.outputUntypedNull(relation.*, index);
        result.columns = output_columns;
        return result;
    }
    if (compiled.statement == .select and compiled.statement.select.windows.len != 0) {
        // Validate even unused WINDOW declarations without evaluating them.
        // Pin the same table for validation and the executable projection.
        const relations = @import("relation_binding.zig");
        var adapter: ?relations.ResolveAdapter = null;
        const pinned = if (compiled.statement.select.table) |name| blk: {
            adapter = .{ .backend = backend, .table = try backend.vtable.resolve(backend.ptr, allocator, name, .read) };
            break :blk adapter.?.iface();
        } else backend;
        var validation = compiled.*;
        const query = &validation.statement.select;
        var projections: std.ArrayList(ast.Projection) = .empty;
        try projections.appendSlice(allocator, query.columns);
        if (query.count_all) {
            const count = try allocator.create(ast.Scalar);
            count.* = .{ .call = .{ .name = "count", .args = &.{}, .star = true } };
            try projections.append(allocator, .{ .expression = count });
        }
        for (query.windows) |definition| {
            const row_number = try allocator.create(ast.Scalar);
            row_number.* = .{ .call = .{ .name = "row_number", .args = &.{}, .window = definition.window } };
            try projections.append(allocator, .{ .expression = row_number });
        }
        query.columns = try projections.toOwnedSlice(allocator);
        query.windows = &.{};
        query.count_all = false;
        query.order_by = &.{};
        const validated = try bindInternal(allocator, pinned, &validation, explicit_parameter_types);
        var executable = compiled.*;
        executable.statement.select.windows = &.{};
        return bindInternal(allocator, pinned, &executable, validated.parameter_types);
    }
    if (@import("ddl_runtime.zig").accepts(compiled.statement)) {
        if (compiled.parameter_count != 0) return error.InvalidSqlParameters;
        if (backend.vtable.ddl == null) return error.UnsupportedSqlExecution;
        try backend.vtable.checkpoint(backend.ptr);
        if (compiled.statement == .create_table) {
            const schema_json = try @import("ddl_runtime.zig").createSchemaAlloc(allocator, compiled.statement.create_table);
            allocator.free(schema_json);
        }
        // DDL has no result row or existing table binding. Describe must never
        // create/drop an object merely to discover its protocol metadata.
        return .{ .table = null, .action = .admin, .columns = &.{}, .parameter_types = &.{}, .json_literals = .empty };
    }
    if (compiled.statement == .select and @import("window_binding.zig").accepts(compiled.statement.select)) {
        const window = try allocator.create(@import("window_binding.zig").Bound);
        window.* = try @import("window_binding.zig").bind(allocator, backend, compiled, explicit_parameter_types);
        const columns = try allocator.alloc(Column, window.outputs.len);
        for (columns, window.names, window.outputs) |*column, name, program| column.* = .{ .name = name, .type = internalKind(program.output_type.kind), .element_type = program.output_type.element_type, .untyped_null = program.output_type.kind == null };
        return .{ .table = window.input.table, .action = .read, .columns = columns, .parameter_types = window.input.parameter_types, .json_literals = .empty, .window = window };
    }
    if (compiled.statement == .select and @import("aggregate_binding.zig").accepts(compiled.statement.select)) {
        try backend.vtable.checkpoint(backend.ptr);
        const table = if (compiled.statement.select.table) |name| try backend.vtable.resolve(backend.ptr, allocator, name, .read) else null;
        const parameters = try allocator.alloc(?ast.ColumnType, compiled.parameter_count);
        @memset(parameters, null);
        @memcpy(parameters[0..explicit_parameter_types.len], explicit_parameter_types);
        const aggregate = try allocator.create(@import("aggregate_binding.zig").Bound);
        aggregate.* = try @import("aggregate_binding.zig").bindWithInvocation(allocator, table, compiled.statement.select, parameters, backend.settings_view, backend.parameter_invocation);
        const columns = try allocator.alloc(Column, aggregate.outputs.len);
        for (columns, aggregate.names, aggregate.outputs) |*column, name, program| column.* = .{ .name = name, .type = internalKind(program.output_type.kind), .element_type = program.output_type.element_type, .untyped_null = program.output_type.kind == null };
        var json_literals: std.StringHashMapUnmanaged(Json) = .empty;
        if (table) |definition| {
            const contexts = try allocator.alloc(?ast.ColumnType, parameters.len);
            @memset(contexts, null);
            var context: Context = .{ .allocator = allocator, .backend = backend, .table = definition, .parameters = parameters, .contexts = contexts };
            try context.predicate(compiled.statement.select.predicate);
            json_literals = context.json_literals;
        }
        return .{ .table = table, .action = .read, .columns = columns, .parameter_types = parameters, .json_literals = json_literals, .scalars = aggregate.input, .aggregate = aggregate };
    }
    if (compiled.statement == .select and compiled.statement.select.table == null) {
        try backend.vtable.checkpoint(backend.ptr);
        return bindConstantSelect(allocator, compiled, explicit_parameter_types, backend.settings_view, backend.parameter_fallback_types, backend.parameter_invocation);
    }
    if (compiled.statement == .merge) {
        try backend.vtable.checkpoint(backend.ptr);
        const merge = compiled.statement.merge;
        const target_table = try backend.vtable.resolve(backend.ptr, allocator, merge.table, .read_write);
        const bound = try allocator.create(@import("merge_mutation.zig").Candidates);
        bound.* = try @import("merge_mutation.zig").bindCandidates(allocator, backend, target_table, compiled, explicit_parameter_types);
        return .{ .table = target_table, .action = .read_write, .columns = if (bound.returning_plan) |plan| plan.columns else &.{}, .parameter_types = bound.parameter_types, .json_literals = .empty, .merge_mutation = bound };
    }
    const target: struct { name: ast.Name, action: catalog.Action } = switch (compiled.statement) {
        .select => |statement| .{ .name = statement.table.?, .action = .read },
        .insert => |statement| .{ .name = statement.table, .action = if (statement.returning != null or statement.conflict != null) .read_write else .write },
        .update => |statement| .{ .name = statement.table, .action = .read_write },
        .delete => |statement| .{ .name = statement.table, .action = .read_write },
        else => return error.UnsupportedSqlExecution,
    };
    try backend.vtable.checkpoint(backend.ptr);
    const table = try backend.vtable.resolve(backend.ptr, allocator, target.name, target.action);
    const parameters = try allocator.alloc(?ast.ColumnType, compiled.parameter_count);
    @memset(parameters, null);
    @memcpy(parameters[0..explicit_parameter_types.len], explicit_parameter_types);
    const joined = switch (compiled.statement) {
        .update => |statement| statement.source != null,
        .delete => |statement| statement.source != null,
        else => false,
    };
    const returning_columns: ?[]const ast.Projection = switch (compiled.statement) {
        .insert => |statement| statement.returning,
        .update => |statement| statement.returning,
        .delete => |statement| statement.returning,
        else => null,
    };
    var projection_name = target.name;
    switch (compiled.statement) {
        .insert => |statement| if (statement.alias) |alias| {
            projection_name.table = alias;
        },
        .update => |statement| if (statement.alias) |alias| {
            projection_name.table = alias;
        },
        .delete => |statement| if (statement.alias) |alias| {
            projection_name.table = alias;
        },
        else => {},
    }
    const target_aliased = switch (compiled.statement) {
        .insert => |statement| statement.alias != null,
        .update => |statement| statement.alias != null,
        .delete => |statement| statement.alias != null,
        else => false,
    };
    const relational_returning = returningReads(returning_columns);
    const returning_select: ?ast.Select = if (returning_columns) |projections| selection: {
        if (!relational_returning) break :selection .{ .table = target.name, .columns = try @import("relation_binding.zig").normalizeTargetProjection(allocator, backend, table, projection_name, target_aliased, projections) };
        const source = try allocator.create(ast.Relation);
        source.* = .{ .table = .{ .name = target.name, .alias = if (target_aliased) projection_name.table else null, .prepared_rows = true } };
        break :selection .{ .source = source, .columns = projections, .ctes = switch (compiled.statement) {
            .update => |mutation| mutation.ctes,
            .delete => |mutation| mutation.ctes,
            else => &.{},
        } };
    } else null;
    if (returning_select) |selection| {
        if (@import("aggregate_binding.zig").accepts(selection)) return error.UnsupportedSqlShape;
        var adapter: @import("relation_binding.zig").TargetResolveAdapter = .{ .backend = backend, .table = table, .name = target.name };
        try @import("relation_binding.zig").inferExpected(allocator, adapter.iface(), selection, parameters, &.{});
    }
    const contexts = try allocator.alloc(?ast.ColumnType, compiled.parameter_count);
    defer allocator.free(contexts);
    @memset(contexts, null);
    var context: Context = .{ .allocator = allocator, .backend = backend, .table = table, .parameters = parameters, .contexts = contexts };
    if (!joined) context.scalars = try bound_scalars.bindWithInvocation(allocator, table, compiled.statement, parameters, backend.settings_view, backend.parameter_fallback_types, backend.parameter_invocation);
    const columns: []const Column = if (joined) &.{} else switch (compiled.statement) {
        .select => |statement| try context.select(statement),
        .insert => |statement| blk: {
            try context.insert(statement);
            break :blk &.{};
        },
        .update => |statement| blk: {
            try context.update(statement);
            break :blk &.{};
        },
        .delete => |statement| blk: {
            try context.predicate(statement.predicate);
            break :blk &.{};
        },
        else => unreachable,
    };
    var result: BoundStatement = .{ .table = table, .action = target.action, .columns = columns, .parameter_types = parameters, .json_literals = context.json_literals, .scalars = context.scalars, .order_keys = context.order_keys, .primary_order = context.primary_order };
    if (joined) {
        const bound = try allocator.create(@import("joined_mutation.zig").Bound);
        bound.* = try @import("joined_mutation.zig").bind(allocator, backend, table, compiled, parameters);
        result.joined_mutation = bound;
        result.parameter_types = bound.input.parameter_types;
    }
    if (compiled.statement == .insert) if (compiled.statement.insert.source) |source| {
        const insertion = compiled.statement.insert;
        var source_query = source.*;
        if (insertion.values_source_rows.len != 0) {
            var row_index: usize = 0;
            source_query = (try typedValuesSource(allocator, source, insertion, table, &row_index)).*;
            if (row_index != insertion.values_source_rows.len) return error.InvalidSqlParameters;
        }
        // Source-aware RETURNING must capture simple INSERT SELECT inputs in
        // the same read set as its own physical subqueries, too.
        if (relational_returning and source_query.source == null) if (source_query.table) |name| {
            const relation = try allocator.create(ast.Relation);
            relation.* = .{ .table = .{ .name = name } };
            source_query.source = relation;
            source_query.table = null;
        };
        // Assignment context supplies the type of otherwise-untyped positional
        // parameters. The SELECT binder retains all source authorization and
        // immutable catalog identities separately from the target binding.
        const capture_count = if (insertion.conflict) |clause| clause.capture_count else 0;
        const expected = try allocator.alloc(ast.ColumnType, insertion.columns.len + capture_count);
        for (insertion.columns, expected[0..insertion.columns.len]) |name, *kind| kind.* = (try table.column(name)).type;
        if (capture_count != 0) for (insertion.conflict.?.assignments) |assignment| if (assignment.capture_ordinal) |ordinal| {
            for (ordinal..ordinal + assignment.capture_span) |capture_index| expected[insertion.columns.len + capture_index] = (try table.column(assignment.field)).type;
        };
        try @import("relation_binding.zig").inferExpected(allocator, backend, source_query, parameters, expected);
        const lowered: compiler.Compiled = .{ .arena = undefined, .statement = .{ .select = source_query }, .parameter_count = compiled.parameter_count };
        const bound = try allocator.create(BoundStatement);
        bound.* = try bindInternal(allocator, backend, &lowered, parameters);
        if (bound.columns.len != expected.len) return error.InvalidSqlParameters;
        for (bound.columns[0..insertion.columns.len], insertion.columns) |source_column, name| {
            const destination = try table.column(name);
            const untyped_null = source_column.untyped_null;
            if (!untyped_null and source_column.type != destination.type and !(source_column.type == .integer and destination.type == .number) and !(insertion.values_source_rows.len != 0 and source_column.type == .string and (destination.type == .datetime or destination.type == .json or destination.type == .uuid))) return error.SqlTypeMismatch;
        }
        result.insert_source = bound;
        result.parameter_types = bound.parameter_types;
    };
    if (compiled.statement == .insert) if (compiled.statement.insert.conflict) |clause| {
        const capture_types = try allocator.alloc(ast.ColumnType, clause.capture_count);
        if (clause.capture_count != 0) {
            const source = result.insert_source orelse return error.InvalidSqlBackendResponse;
            for (source.columns[compiled.statement.insert.columns.len..], capture_types) |column, *kind| kind.* = column.type;
        }
        result.conflict = try @import("conflict.zig").bind(allocator, backend, table, projection_name, target_aliased, clause, parameters, capture_types);
    };
    if (returning_select) |selection| {
        var adapter: @import("relation_binding.zig").TargetResolveAdapter = .{ .backend = backend, .table = table, .name = target.name };
        const returning_compiled: compiler.Compiled = .{ .arena = undefined, .statement = .{ .select = selection }, .parameter_count = compiled.parameter_count };
        const returning_bound = try allocator.create(BoundStatement);
        returning_bound.* = try bindInternal(allocator, adapter.iface(), &returning_compiled, result.parameter_types);
        result.returning = returning_bound;
        result.returning_projections = selection.columns;
        if (relational_returning) result.returning_query = selection;
        result.columns = returning_bound.columns;
        result.parameter_types = returning_bound.parameter_types;
    }
    return result;
}

fn returningReads(projections: ?[]const ast.Projection) bool {
    for (projections orelse return false) |projection| if (projection.expression) |expression| {
        if (@import("subquery_lowering.zig").has(expression)) return true;
    };
    return false;
}

fn bindConstantSelect(alloc: std.mem.Allocator, compiled: *const compiler.Compiled, hints: []const ?ast.ColumnType, settings: ?*const @import("setting_catalog.zig").View, fallbacks: []const ?ast.ColumnType, invocation: ?*@import("parameter_binding.zig").Invocation) !BoundStatement {
    const statement = compiled.statement.select;
    if (!statement.count_all and statement.columns.len == 0) return error.UndefinedColumn;
    const parameters = try alloc.alloc(?ast.ColumnType, compiled.parameter_count);
    @memset(parameters, null);
    @memcpy(parameters[0..hints.len], hints);
    for ([_]?ast.Value{ statement.limit, statement.offset }) |optional| if (optional) |node| {
        switch (node) {
            .integer => {},
            .null => {},
            .parameter => |slot| {
                if (slot == 0 or slot > parameters.len) return error.InvalidSqlParameters;
                if (parameters[slot - 1]) |kind| if (kind != .integer) return error.ConflictingSqlParameterTypes;
                parameters[slot - 1] = .integer;
            },
            else => return error.InvalidSqlLimit,
        }
    };
    const scalars = try bound_scalars.bindWithInvocation(alloc, null, compiled.statement, parameters, settings, fallbacks, invocation);
    const columns = try alloc.alloc(Column, if (statement.count_all) 1 else statement.columns.len);
    if (statement.count_all) {
        columns[0] = .{ .name = try alloc.dupe(u8, statement.count_alias orelse "count"), .type = .integer };
    } else for (statement.columns, scalars.projections, columns) |projection, program, *column| {
        const expression = program orelse return error.UndefinedColumn;
        column.* = .{ .name = try alloc.dupe(u8, projection.alias orelse "?column?"), .type = internalKind(expression.output_type.kind), .element_type = expression.output_type.element_type, .untyped_null = expression.output_type.kind == null };
    }
    // Ordering a singleton changes nothing, but names must still resolve.
    for (statement.order_by) |order| {
        if (order.expression != null) continue;
        if (order.position) |position| {
            if (position == 0 or position > columns.len) return error.UndefinedColumn;
            continue;
        }
        var found: usize = 0;
        for (columns) |column| if (std.mem.eql(u8, column.name, order.field)) {
            found += 1;
        };
        if (found == 0) return error.UndefinedColumn;
        if (found > 1) return error.AmbiguousSqlColumn;
    }
    return .{ .table = null, .action = .read, .columns = columns, .parameter_types = parameters, .json_literals = .empty, .scalars = scalars };
}

const Context = struct {
    scalars: bound_scalars.Bound = .{},
    order_keys: []const OrderKey = &.{},
    primary_order: bool = false,
    allocator: std.mem.Allocator,
    backend: catalog.Backend,
    table: catalog.Table,
    parameters: []?ast.ColumnType,
    contexts: []?ast.ColumnType,
    predicate_terms: usize = 0,
    json_literals: std.StringHashMapUnmanaged(Json) = .empty,

    fn parameter(self: *Context, index: u32, expected: ast.ColumnType) !void {
        if (index == 0 or index > self.parameters.len) return error.InvalidSqlParameters;
        const slot = index - 1;
        if (self.contexts[slot]) |prior| {
            if (prior != expected) return error.ConflictingSqlParameterTypes;
        } else self.contexts[slot] = expected;
        if (self.parameters[slot]) |provided| {
            // Only lossless/declared native widening is implicit. In
            // particular, an explicitly typed text parameter does not become
            // an integer merely because some future value might parse as one.
            if (provided != expected and !(provided == .integer and expected == .number)) return error.ConflictingSqlParameterTypes;
        } else self.parameters[slot] = expected;
    }

    fn value(self: *Context, node: ast.Value, column: catalog.Column, assignment: bool) !void {
        if (node == .parameter) return self.parameter(node.parameter, column.type);
        const typed = if (node == .string and column.type == .json) json: {
            const entry = try self.json_literals.getOrPut(self.allocator, node.string);
            if (!entry.found_existing) {
                entry.key_ptr.* = try self.allocator.dupe(u8, node.string);
                entry.value_ptr.* = try bindLiteral(self.allocator, node, column.type);
            }
            break :json entry.value_ptr.*;
        } else try bindLiteral(self.allocator, node, column.type);
        if (assignment and typed == .null and !column.nullable and !(node == .string and column.type == .json)) return error.SqlNotNullViolation;
    }

    fn predicate(self: *Context, maybe_node: ?*const ast.Predicate) anyerror!void {
        const node = maybe_node orelse return;
        switch (node.*) {
            .comparison => |comparison| {
                const column = try self.table.column(comparison.field);
                if (std.mem.eql(u8, column.name, "_id")) {
                    try self.value(comparison.value, column, false);
                    if (comparison.value == .string and !std.unicode.utf8ValidateSlice(comparison.value.string)) return error.SqlTypeMismatch;
                    return;
                }
                try self.value(comparison.value, column, false);
                self.predicate_terms += 1;
            },
            .is_null => |test_null| {
                const column = try self.table.column(test_null.field);
                if (std.mem.eql(u8, column.name, "_id")) return;
                self.predicate_terms += 1;
            },
            .conjunction => |both| {
                try self.predicate(both.left);
                try self.predicate(both.right);
            },
            .disjunction => |both| {
                try self.predicate(both.left);
                try self.predicate(both.right);
            },
            .negation => |inner| try self.predicate(inner),
            .scalar => {}, // Already typed by the shared scalar binder.
        }
        if (self.predicate_terms > 256) return error.SqlProgramLimitExceeded;
    }

    fn rowBound(self: *Context, maybe_node: ?ast.Value) !void {
        const node = maybe_node orelse return;
        try self.value(node, .{ .name = "limit", .path = "limit", .type = .integer }, false);
        switch (node) {
            .integer => {},
            .null => {},
            .parameter => {},
            else => return error.InvalidSqlLimit,
        }
    }

    fn select(self: *Context, statement: ast.Select) ![]const Column {
        try self.predicate(statement.predicate);
        try self.rowBound(statement.limit);
        try self.rowBound(statement.offset);
        self.order_keys = try bindOrder(self.allocator, self.table, statement);
        if (self.order_keys.len == 1 and !self.order_keys[0].descending and !statement.count_all) {
            const field = switch (self.order_keys[0].source) {
                .column => |column| column.name,
                .output => |index| if (statement.columns.len == 0) self.table.columns[index].name else statement.columns[index].field,
                .expression => "",
            };
            self.primary_order = std.mem.eql(u8, field, "_id");
        }
        if (statement.count_all) {
            const columns = try self.allocator.alloc(Column, 1);
            columns[0] = .{ .name = try self.allocator.dupe(u8, statement.count_alias orelse "count"), .type = .integer, .element_type = .int64 };
            return columns;
        }
        if (statement.columns.len == 0) {
            if (self.table.columns.len > 256) return error.SqlProgramLimitExceeded;
            const columns = try self.allocator.alloc(Column, self.table.columns.len);
            for (self.table.columns, columns) |column, *output| output.* = .{ .name = try self.allocator.dupe(u8, column.name), .type = internalKind(column.type), .element_type = column.element_type };
            return columns;
        }
        const columns = try self.allocator.alloc(Column, statement.columns.len);
        var native_fields: std.StringHashMapUnmanaged(void) = .empty;
        defer native_fields.deinit(self.allocator);
        for (statement.columns, columns, 0..) |projection, *output, index| {
            if (projection.expression != null) {
                const program = self.scalars.projections[index] orelse return error.InvalidSqlBackendResponse;
                output.* = .{ .name = try self.allocator.dupe(u8, projection.alias orelse "?column?"), .type = internalKind(program.output_type.kind), .element_type = program.output_type.element_type, .untyped_null = program.output_type.kind == null };
                continue;
            }
            const column = try self.table.column(projection.field);
            if (!std.mem.eql(u8, column.name, "_id")) {
                _ = try native_fields.getOrPut(self.allocator, column.path);
                if (native_fields.count() > 256 and !statement.internal_projection) return error.SqlProgramLimitExceeded;
            }
            output.* = .{ .name = try self.allocator.dupe(u8, projection.alias orelse column.name), .type = internalKind(column.type), .element_type = column.element_type };
        }
        return columns;
    }

    fn insert(self: *Context, statement: ast.Insert) !void {
        const columns = try self.allocator.alloc(catalog.Column, statement.columns.len);
        defer self.allocator.free(columns);
        const value_rows = if (statement.values_source_rows.len != 0) statement.values_source_rows else statement.rows;
        if (statement.defaults.len != 0) {
            if (statement.defaults.len != value_rows.len) return error.InvalidSqlParameters;
            for (statement.defaults) |mask| if (mask.len != columns.len) return error.InvalidSqlParameters;
        }
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        defer seen.deinit(self.allocator);
        var identity_column: ?usize = null;
        for (statement.columns, columns, 0..) |name, *column, i| {
            if ((try seen.getOrPut(self.allocator, name)).found_existing) return error.DuplicateColumn;
            column.* = try self.table.column(name);
            if (column.generated) {
                if (value_rows.len == 0) return error.SqlGeneratedColumnWrite;
                for (value_rows, 0..) |_, row_index| if (!statement.isDefault(row_index, i)) return error.SqlGeneratedColumnWrite;
            }
            if (std.mem.eql(u8, name, "_id")) identity_column = i;
        }
        if (self.backend.vtable.generate_row_id == null) {
            const key_column = identity_column orelse return error.SqlRowIdentityRequired;
            for (value_rows, 0..) |_, row_index| if (statement.isDefault(row_index, key_column)) return error.SqlRowIdentityRequired;
        }
        var literal_keys: std.StringHashMapUnmanaged(void) = .empty;
        defer literal_keys.deinit(self.allocator);
        for (statement.rows, 0..) |row, row_index| {
            try self.backend.vtable.checkpoint(self.backend.ptr);
            if (row.len != columns.len) return error.InvalidSqlParameters;
            for (columns, row, 0..) |column, node, cell_index| {
                if (statement.isDefault(row_index, cell_index)) continue;
                if (self.scalars.insert_rows.len != 0 and self.scalars.insert_rows[row_index][cell_index] != null) continue;
                try self.value(node, column, true);
            }
            const key_column = identity_column orelse continue;
            if (statement.isDefault(row_index, key_column)) continue;
            if (self.scalars.insert_rows.len != 0 and self.scalars.insert_rows[row_index][key_column] != null) continue;
            switch (row[key_column]) {
                .parameter => {},
                .string => |key| {
                    if (key.len == 0) return error.SqlRowIdentityRequired;
                    if (!std.unicode.utf8ValidateSlice(key)) return error.SqlTypeMismatch;
                    if ((try literal_keys.getOrPut(self.allocator, key)).found_existing and (statement.conflict == null or !@import("conflict.zig").allowsDuplicateKeys(statement.conflict.?))) return error.DuplicateSqlRow;
                },
                else => return error.SqlRowIdentityRequired,
            }
        }
    }

    fn update(self: *Context, statement: ast.Update) !void {
        try self.predicate(statement.predicate);
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        defer seen.deinit(self.allocator);
        for (statement.assignments) |assignment| {
            const column = try self.table.column(assignment.field);
            if (std.mem.eql(u8, column.name, "_id")) return error.UnsupportedSqlExecution;
            if (column.generated) return error.SqlGeneratedColumnWrite;
            if ((try seen.getOrPut(self.allocator, assignment.field)).found_existing) return error.DuplicateColumn;
            if (assignment.expression == null) try self.value(assignment.value, column, true);
        }
        // Execution's patch-by-replacement plan reads only untouched columns;
        // replacing a wide table must not require projecting overwritten data.
        var untouched: usize = 0;
        for (self.table.columns) |column| {
            if (!column.generated and !seen.contains(column.name)) untouched += 1;
        }
        if (untouched > 256) return error.SqlProgramLimitExceeded;
    }
};

/// Bind an SQL literal, whose string tokens are untyped input text rather than
/// already typed JSON parameter values. In particular '{}'/JSON means an object,
/// while an externally supplied JSON string parameter remains a JSON string.
/// Parsed data belongs to the caller's request arena.
pub fn bindLiteral(allocator: std.mem.Allocator, node: ast.Value, kind: ast.ColumnType) !Json {
    if (node == .parameter) return error.InvalidSqlParameters;
    if (node == .string and kind == .uuid) return .{ .string = @import("../common/uuid.zig").canonicalAlloc(allocator, node.string) catch |err| switch (err) {
        error.InvalidUuid => return error.SqlTypeMismatch,
        else => return err,
    } };
    if (node == .string and kind == .json) {
        // Admit nesting before constructing the tree: otherwise a 1 MiB SQL
        // string of brackets could allocate a huge dynamic JSON value before
        // the later copy's depth guard gets a chance to reject it.
        try admitJsonLiteral(allocator, node.string);
        const parsed = std.json.parseFromSliceLeaky(Json, allocator, node.string, .{ .parse_numbers = false, .allocate = .alloc_always }) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return error.SqlTypeMismatch,
        };
        return parsed;
    }
    return coerce(switch (node) {
        .null => .null,
        .boolean => |value_| .{ .bool = value_ },
        .integer => |value_| .{ .integer = value_ },
        .number => |value_| .{ .float = value_ },
        .string => |value_| .{ .string = value_ },
        .parameter => unreachable,
    }, kind);
}

fn admitJsonLiteral(allocator: std.mem.Allocator, text: []const u8) !void {
    var work: @import("json_order.zig").Budget = .{};
    @import("json_order.zig").admitText(allocator, text, &work) catch |err| return switch (err) {
        error.OutOfMemory, error.SqlProgramLimitExceeded => err,
        else => error.SqlTypeMismatch,
    };
}

/// Output aliases have SQL precedence over source columns in ORDER BY.
fn bindOrder(alloc: std.mem.Allocator, table: catalog.Table, statement: ast.Select) ![]const OrderKey {
    const keys = try alloc.alloc(OrderKey, statement.order_by.len);
    for (statement.order_by, keys, 0..) |order, *key, order_index| {
        if (order.position) |position| {
            const count: usize = if (statement.count_all) 1 else if (statement.columns.len == 0) table.columns.len else statement.columns.len;
            if (position == 0 or position > count) return error.UndefinedColumn;
            key.* = .{ .source = .{ .output = position - 1 }, .descending = order.descending, .nulls_first = order.nulls_first };
            continue;
        }
        if (order.expression != null) {
            if (statement.count_all) return error.UnsupportedSqlExecution;
            key.* = .{ .source = .{ .expression = order_index }, .descending = order.descending, .nulls_first = order.nulls_first };
            continue;
        }
        if (statement.count_all) {
            if (!std.mem.eql(u8, order.field, statement.count_alias orelse "count")) return error.UnsupportedSqlExecution;
            key.* = .{ .source = .{ .output = 0 }, .descending = order.descending, .nulls_first = order.nulls_first };
            continue;
        }
        var selected: ?usize = null;
        for (statement.columns, 0..) |projection, index| {
            if (!std.mem.eql(u8, projection.alias orelse projection.field, order.field)) continue;
            if (selected) |prior| {
                const previous = statement.columns[prior];
                if (previous.expression != null or projection.expression != null or !std.mem.eql(u8, previous.field, projection.field)) return error.AmbiguousSqlColumn;
            }
            selected = index;
        }
        key.* = .{ .source = if (selected) |index| .{ .output = index } else .{ .column = try table.column(order.field) }, .descending = order.descending, .nulls_first = order.nulls_first };
    }
    return keys;
}

/// Shared literal/parameter coercion. Exact integer columns never pass through
/// f64. Native number columns intentionally have IEEE-754 semantics.
/// Typed internal boundaries must never coerce an array's JSON placeholder.
/// Array element descriptors are part of the type, not inferred from values.
pub fn coerceDatum(alloc: std.mem.Allocator, raw: @import("scalar.zig").Datum, kind: ast.ColumnType, element_type: ?@import("array_value.zig").ElementType) !@import("scalar.zig").Datum {
    if (raw.sql_null and raw.value != .null) return error.SqlTypeMismatch;
    if (raw.array) |array| {
        if (kind != .array or element_type == null or element_type.? != array.element_type or raw.sql_null or raw.value != .null or raw.patterns != null) return error.SqlTypeMismatch;
        return raw;
    }
    if (kind == .array and (!raw.sql_null or element_type == null)) return error.SqlTypeMismatch;
    var result = raw;
    result.value = try coerceAlloc(alloc, raw.value, kind);
    if (!result.sql_null) if (element_type) |element| {
        const casts = @import("builtin_cast.zig");
        if (kind == .integer and casts.integral(element)) result.value = .{ .integer = try casts.checkedInteger(result.value.integer, element) };
        if (kind == .number and element == .float32) result.value = .{ .float = try casts.floatValue(f32, result.value) };
    };
    return result;
}

/// Native storage uses unsigned epoch nanos; external lake timestamps can be
/// signed. Convert both exactly at the SQL boundary so projection, ordering and
/// scalar evaluation share the same canonical datetime representation.
pub fn coerceAlloc(alloc: std.mem.Allocator, raw: Json, kind: ast.ColumnType) !Json {
    if (kind == .datetime and raw != .null) {
        const datetime = @import("../datetime.zig");
        const ns: i128 = switch (raw) {
            .integer => |value| value,
            .number_string => |text| std.fmt.parseInt(i128, text, 10) catch return error.InvalidSqlDateTime,
            .string => |text| datetime.parseDateTimeToSignedNs(text) orelse return error.InvalidSqlDateTime,
            else => return error.SqlTypeMismatch,
        };
        return .{ .string = datetime.formatDateTimeSignedNsAlloc(alloc, ns) catch |err| switch (err) {
            error.InvalidDateTime => return error.InvalidSqlDateTime,
            else => return err,
        } };
    }
    return coerce(raw, kind);
}

pub fn coerce(raw: Json, kind: ast.ColumnType) !Json {
    if (raw == .null) return .null;
    return switch (kind) {
        .array => error.SqlTypeMismatch,
        .integer => switch (raw) {
            .integer => raw,
            .number_string, .string => |text| .{ .integer = std.fmt.parseInt(i64, text, 10) catch return error.SqlTypeMismatch },
            else => error.SqlTypeMismatch,
        },
        .number => switch (raw) {
            .integer => |value| .{ .float = @floatFromInt(value) },
            .float => |value| if (std.math.isFinite(value)) raw else error.SqlTypeMismatch,
            .number_string, .string => |text| blk: {
                const value = std.fmt.parseFloat(f64, text) catch return error.SqlTypeMismatch;
                if (!std.math.isFinite(value)) return error.SqlTypeMismatch;
                break :blk .{ .float = value };
            },
            else => error.SqlTypeMismatch,
        },
        .boolean => if (raw == .bool) raw else error.SqlTypeMismatch,
        .string, .datetime => if (raw == .string) raw else error.SqlTypeMismatch,
        .uuid => blk: {
            if (raw != .string) return error.SqlTypeMismatch;
            _ = @import("../common/uuid.zig").parse(raw.string) catch return error.SqlTypeMismatch;
            break :blk raw;
        },
        .json => raw,
    };
}

const FakeBackend = struct {
    resolutions: usize = 0,
    action: ?catalog.Action = null,
    forbidden: bool = false,
    cancelled: bool = false,

    fn backend(self: *FakeBackend) catalog.Backend {
        return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint } };
    }
    fn resolve(ptr: *anyopaque, allocator: std.mem.Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
        const self: *FakeBackend = @ptrCast(@alignCast(ptr));
        self.resolutions += 1;
        self.action = action;
        if (self.forbidden) return error.Forbidden;
        try std.testing.expectEqualStrings("things", name.table);
        return .{ .id = 7, .physical_name = try allocator.dupe(u8, "table:immutable"), .schema_version = 9, .columns = &.{
            .{ .name = "name", .path = "name", .type = .string },
            .{ .name = "age", .path = "age", .type = .integer, .nullable = false },
            .{ .name = "enabled", .path = "enabled", .type = .boolean },
            .{ .name = "score", .path = "score", .type = .number },
            .{ .name = "payload", .path = "payload", .type = .json },
            .{ .name = "derived", .path = "derived", .type = .integer, .generated = true },
        } };
    }
    fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.DescriptionMustNotReadRows;
    }
    fn mutate(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
        return error.DescriptionMustNotMutate;
    }
    fn checkpoint(ptr: *anyopaque) !void {
        const self: *FakeBackend = @ptrCast(@alignCast(ptr));
        if (self.cancelled) return error.Cancelled;
    }
};

test "SQL describe owns ordered aliases and complete sparse parameter metadata without execution" {
    var fake: FakeBackend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT _id AS key,age AS years,name,age FROM things WHERE age >= $2 AND name = $3 LIMIT $4 OFFSET $5", .{});
    var result = try describe(std.testing.allocator, fake.backend(), &compiled, &.{});
    compiled.deinit();
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), fake.resolutions);
    try std.testing.expectEqual(catalog.Action.read, fake.action.?);
    try std.testing.expectEqual(@as(usize, 4), result.binding.columns.len);
    try std.testing.expectEqualStrings("key", result.binding.columns[0].name);
    try std.testing.expectEqualStrings("years", result.binding.columns[1].name);
    try std.testing.expectEqual(ast.ColumnType.integer, result.binding.columns[1].type);
    try std.testing.expectEqualSlices(?ast.ColumnType, &.{ null, .integer, .string, .integer, .integer }, result.binding.parameter_types);
    try std.testing.expectEqual(@as(u64, 7), result.binding.table.?.id);
    try std.testing.expectEqual(@as(u32, 9), result.binding.table.?.schema_version);
    try std.testing.expectEqualStrings("table:immutable", result.binding.table.?.physical_name);
}

test "SQL describe INSERT and UPDATE authorize and infer without writes or reads" {
    const cases = [_]struct { sql: []const u8, action: catalog.Action, types: []const ?ast.ColumnType }{
        .{ .sql = "INSERT INTO things (_id,age,name) VALUES ($1,$2,$3)", .action = .write, .types = &.{ .string, .integer, .string } },
        .{ .sql = "UPDATE things SET age=$2 WHERE name=$1", .action = .read_write, .types = &.{ .string, .integer } },
        .{ .sql = "DELETE FROM things WHERE _id=$1", .action = .read_write, .types = &.{.string} },
    };
    for (cases) |case| {
        var fake: FakeBackend = .{};
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try describe(std.testing.allocator, fake.backend(), &compiled, &.{});
        defer result.deinit();
        try std.testing.expectEqual(case.action, fake.action.?);
        try std.testing.expectEqual(@as(usize, 0), result.binding.columns.len);
        try std.testing.expectEqualSlices(?ast.ColumnType, case.types, result.binding.parameter_types);
    }
}

test "SQL describe rejects conflicting parameter contexts and incompatible explicit types" {
    var fake: FakeBackend = .{};
    var conflicting = try compiler.compile(std.testing.allocator, "SELECT * FROM things WHERE age=$1 AND name=$1", .{});
    defer conflicting.deinit();
    try std.testing.expectError(error.ConflictingSqlParameterTypes, describe(std.testing.allocator, fake.backend(), &conflicting, &.{}));
    var constrained = try compiler.compile(std.testing.allocator, "SELECT age FROM things WHERE age=$1", .{});
    defer constrained.deinit();
    try std.testing.expectError(error.ConflictingSqlParameterTypes, describe(std.testing.allocator, fake.backend(), &constrained, &.{.string}));
    try std.testing.expectError(error.InvalidSqlParameters, describe(std.testing.allocator, fake.backend(), &constrained, &.{ .integer, .integer }));
    var widened = try compiler.compile(std.testing.allocator, "SELECT score FROM things WHERE score=$2", .{});
    defer widened.deinit();
    var result = try describe(std.testing.allocator, fake.backend(), &widened, &.{ .boolean, .integer });
    defer result.deinit();
    try std.testing.expectEqualSlices(?ast.ColumnType, &.{ .boolean, .integer }, result.binding.parameter_types);
}

test "SQL whole shape infers nested derived CTE set and assignment parameters before emission" {
    const cases = [_][]const u8{
        "SELECT d.x FROM (SELECT $1::bigint AS x) d UNION SELECT 1",
        "SELECT d.x FROM (SELECT e.x FROM (SELECT $1::bigint AS x) e) d UNION SELECT 1",
        "WITH a AS (SELECT $1::bigint AS x), b AS (SELECT x FROM a) SELECT x FROM b UNION SELECT 1",
        "WITH a(x) AS (SELECT $1::bigint) SELECT l.x FROM a l JOIN a r ON l.x=r.x UNION SELECT 1",
        "SELECT d.x FROM (SELECT $1::bigint AS x) d WHERE d.x=1",
        "SELECT d.x FROM (SELECT $1::bigint AS x) d ORDER BY d.x+1",
        "SELECT d.x FROM (SELECT $1::bigint AS x) d LIMIT $1",
        "SELECT d.x FROM (SELECT $1::bigint AS x) d JOIN things t ON d.x=t.age",
        "SELECT d.x FROM (SELECT $1::bigint AS x UNION SELECT $2) d UNION SELECT 1",
        "INSERT INTO things (_id,age) SELECT 'a',d.x FROM (SELECT $1::bigint AS x) d",
        "INSERT INTO things (_id,age) WITH a AS (SELECT $1::bigint AS x), b AS (SELECT x FROM a) SELECT 'a',x FROM b",
    };
    for (cases) |sql| {
        errdefer std.debug.print("shape inference query: {s}\n", .{sql});
        var fake: FakeBackend = .{};
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try describe(std.testing.allocator, fake.backend(), &compiled, &.{});
        defer result.deinit();
        for (result.binding.parameter_types) |kind| try std.testing.expectEqual(ast.ColumnType.integer, kind.?);
        // Describe must not consume rows and each physical read identity is
        // resolved once across the symbolic and executable binding passes.
        if (std.mem.indexOf(u8, sql, "JOIN things") != null) try std.testing.expectEqual(@as(usize, 1), fake.resolutions);
    }
}

test "SQL set description cannot infer through resolved text boundaries" {
    for ([_][]const u8{
        "SELECT d.x FROM (SELECT $1 AS x) d UNION SELECT 1",
        "WITH a AS (SELECT $1 AS x) SELECT d.x FROM (SELECT x FROM a) d UNION SELECT 1",
        "SELECT NULL AS x UNION SELECT NULL UNION SELECT $1 UNION SELECT 1",
        "SELECT $1 AS x UNION SELECT NULL UNION SELECT 1",
        "SELECT COALESCE($1,NULL) AS x UNION SELECT 1",
        "WITH n AS (SELECT NULL AS x) SELECT COALESCE(n.x,$1) AS x FROM n UNION SELECT 1",
    }) |sql| {
        var fake: FakeBackend = .{};
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.SqlTypeMismatch, describe(std.testing.allocator, fake.backend(), &compiled, &.{}));
    }
    for ([_][]const u8{ "SELECT $1 AS x UNION SELECT 1 UNION SELECT 1.5", "SELECT 1.5 AS x UNION SELECT 1 UNION SELECT $1" }, 0..) |sql, index| {
        var fake: FakeBackend = .{};
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try describe(std.testing.allocator, fake.backend(), &compiled, &.{});
        defer result.deinit();
        try std.testing.expectEqual(if (index == 0) ast.ColumnType.integer else ast.ColumnType.number, result.binding.parameter_types[0].?);
    }
}

test "SQL shape binding releases partial allocations" {
    const Check = struct {
        fn run(alloc: std.mem.Allocator, compiled: *const compiler.Compiled) !void {
            var fake: FakeBackend = .{};
            var result = try describe(alloc, fake.backend(), compiled, &.{});
            defer result.deinit();
        }
    };
    var compiled = try compiler.compile(std.testing.allocator, "WITH a AS (SELECT $1::bigint AS x) SELECT d.x FROM (SELECT x FROM a) d UNION SELECT 1", .{});
    defer compiled.deinit();
    // Force deterministic growth rather than depending on whether the backing
    // allocator happens to resize an arena chunk at a particular address.
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Check.run, .{&compiled});
}

test "SQL RETURNING Describe uses authorized target and output parameter context" {
    const cases = [_][]const u8{
        "INSERT INTO things (_id,age) VALUES ('a',1) RETURNING age+1 AS next_age,$1+age AS adjusted",
        "UPDATE things SET age=age+1 RETURNING age,$1+age AS adjusted",
        "DELETE FROM things WHERE age=1 RETURNING age,$1+age AS adjusted",
        "DELETE FROM things WHERE age=1 RETURNING things.age,$1+things.age AS adjusted",
    };
    for (cases) |sql| {
        var fake: FakeBackend = .{};
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try describe(std.testing.allocator, fake.backend(), &compiled, &.{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), fake.resolutions);
        try std.testing.expectEqual(catalog.Action.read_write, result.binding.action);
        try std.testing.expect(result.binding.returning != null);
        try std.testing.expectEqual(@as(usize, 2), result.binding.columns.len);
        try std.testing.expectEqualStrings("adjusted", result.binding.columns[1].name);
        try std.testing.expectEqualSlices(?ast.ColumnType, &.{.integer}, result.binding.parameter_types);
    }
    var fake: FakeBackend = .{};
    var invalid = try compiler.compile(std.testing.allocator, "DELETE FROM things RETURNING count(*)", .{});
    defer invalid.deinit();
    try std.testing.expectError(error.UnsupportedSqlShape, describe(std.testing.allocator, fake.backend(), &invalid, &.{}));
}

test "SQL describe binds NULL without guessing values and rejects missing columns" {
    var fake: FakeBackend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT name FROM things WHERE name=NULL AND enabled IS NOT NULL AND age=$1", .{});
    defer compiled.deinit();
    var result = try describe(std.testing.allocator, fake.backend(), &compiled, &.{});
    defer result.deinit();
    try std.testing.expectEqualSlices(?ast.ColumnType, &.{.integer}, result.binding.parameter_types);
    var invalid = try compiler.compile(std.testing.allocator, "SELECT missing FROM things", .{});
    defer invalid.deinit();
    try std.testing.expectError(error.UndefinedColumn, describe(std.testing.allocator, fake.backend(), &invalid, &.{}));
    var null_write = try compiler.compile(std.testing.allocator, "UPDATE things SET age=NULL", .{});
    defer null_write.deinit();
    try std.testing.expectError(error.SqlNotNullViolation, describe(std.testing.allocator, fake.backend(), &null_write, &.{}));
}

test "SQL describe ORDER BY shares execution alias rules and unsupported gates" {
    const cases = [_]struct { sql: []const u8, failure: ?anyerror }{
        .{ .sql = "SELECT _id AS key FROM things ORDER BY key", .failure = null },
        .{ .sql = "SELECT count(*) AS total FROM things ORDER BY total DESC", .failure = null },
        .{ .sql = "SELECT age AS _id FROM things ORDER BY _id", .failure = null },
        .{ .sql = "SELECT _id AS key,age AS key FROM things ORDER BY key", .failure = error.AmbiguousSqlColumn },
        .{ .sql = "SELECT count(*) FROM things ORDER BY _id", .failure = error.SqlGroupingError },
        .{ .sql = "DELETE FROM things WHERE age=1 OR age=2", .failure = null },
        .{ .sql = "UPDATE things SET _id='x'", .failure = error.UnsupportedSqlExecution },
        .{ .sql = "INSERT INTO things (age) VALUES(1)", .failure = error.SqlRowIdentityRequired },
        .{ .sql = "SELECT * FROM things WHERE _id > 'a'", .failure = null },
    };
    for (cases) |case| {
        var fake: FakeBackend = .{};
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        if (case.failure) |failure| {
            try std.testing.expectError(failure, describe(std.testing.allocator, fake.backend(), &compiled, &.{}));
        } else {
            var result = try describe(std.testing.allocator, fake.backend(), &compiled, &.{});
            result.deinit();
        }
    }
}

test "SQL describe propagates authorization and cancellation before any row work" {
    var compiled = try compiler.compile(std.testing.allocator, "DELETE FROM things", .{});
    defer compiled.deinit();
    var fake: FakeBackend = .{ .forbidden = true };
    try std.testing.expectError(error.Forbidden, describe(std.testing.allocator, fake.backend(), &compiled, &.{}));
    fake.cancelled = true;
    const resolved = fake.resolutions;
    try std.testing.expectError(error.Cancelled, describe(std.testing.allocator, fake.backend(), &compiled, &.{}));
    try std.testing.expectEqual(resolved, fake.resolutions);
}

test "SQL describe releases every partial allocation on failure" {
    const Check = struct {
        fn run(alloc: std.mem.Allocator, compiled: *const compiler.Compiled) !void {
            var fake: FakeBackend = .{};
            var result = try describe(alloc, fake.backend(), compiled, &.{});
            defer result.deinit();
        }
    };
    var compiled = try compiler.compile(std.testing.allocator, "INSERT INTO things (_id,age,name,payload) VALUES ($1,$2,$3,'{\"n\":1}'),('next',42,'Ada','{\"n\":1}')", .{});
    defer compiled.deinit();
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{&compiled});
}

test "SQL describe bounds native projection but preserves repeated SQL outputs and wide replacements" {
    const Wide = struct {
        fn resolve(_: *anyopaque, allocator: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
            const columns = try allocator.alloc(catalog.Column, 257);
            for (columns, 0..) |*column, i| {
                const name = try std.fmt.allocPrint(allocator, "c{d}", .{i});
                column.* = .{ .name = name, .path = name, .type = .integer };
            }
            return .{ .id = 8, .physical_name = "wide", .schema_version = 3, .columns = columns };
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    const backend: catalog.Backend = .{ .ptr = &dummy, .vtable = &.{ .resolve = Wide.resolve, .scan = FakeBackend.scan, .mutate = FakeBackend.mutate, .checkpoint = Wide.checkpoint } };
    var star = try compiler.compile(std.testing.allocator, "SELECT * FROM wide", .{});
    defer star.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, describe(std.testing.allocator, backend, &star, &.{}));
    var source = std.ArrayList(u8).empty;
    defer source.deinit(std.testing.allocator);
    try source.appendSlice(std.testing.allocator, "SELECT c0");
    for (0..299) |_| try source.appendSlice(std.testing.allocator, ",c0");
    try source.appendSlice(std.testing.allocator, " FROM wide");
    var repeated = try compiler.compile(std.testing.allocator, source.items, .{});
    defer repeated.deinit();
    var described = try describe(std.testing.allocator, backend, &repeated, &.{});
    defer described.deinit();
    try std.testing.expectEqual(@as(usize, 300), described.binding.columns.len);
    var replacement = try compiler.compile(std.testing.allocator, "UPDATE wide SET c0=$1", .{});
    defer replacement.deinit();
    var update = try describe(std.testing.allocator, backend, &replacement, &.{});
    defer update.deinit();
    try std.testing.expectEqualSlices(?ast.ColumnType, &.{.integer}, update.binding.parameter_types);
}

test "SQL describe validates identity predicates without inventing execution values" {
    var fake: FakeBackend = .{};
    for ([_][]const u8{
        "SELECT _id FROM things WHERE _id IS NULL",
        "SELECT _id FROM things WHERE _id IS NOT NULL",
        "SELECT _id FROM things WHERE _id=NULL",
        "SELECT _id FROM things WHERE _id=''",
        "SELECT _id FROM things WHERE _id=$1",
    }) |source| {
        var compiled = try compiler.compile(std.testing.allocator, source, .{});
        defer compiled.deinit();
        var result = try describe(std.testing.allocator, fake.backend(), &compiled, &.{});
        defer result.deinit();
        try std.testing.expectEqual(ast.ColumnType.string, result.binding.columns[0].type);
    }
    var duplicate = try compiler.compile(std.testing.allocator, "INSERT INTO things (_id,age) VALUES ('x',1),('x',2)", .{});
    defer duplicate.deinit();
    try std.testing.expectError(error.DuplicateSqlRow, describe(std.testing.allocator, fake.backend(), &duplicate, &.{}));
}

test "SQL JSON literal coercion preserves exact number text and typed parameters remain values" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const literal = try bindLiteral(arena.allocator(), .{ .string = "{\"n\":9007199254740993,\"max\":18446744073709551615}" }, .json);
    try std.testing.expect(literal == .object);
    try std.testing.expectEqualStrings("9007199254740993", literal.object.get("n").?.number_string);
    try std.testing.expectEqualStrings("18446744073709551615", literal.object.get("max").?.number_string);
    const scalar = try bindLiteral(arena.allocator(), .{ .string = "\"plain text\"" }, .json);
    try std.testing.expectEqualStrings("plain text", scalar.string);
    const typed_parameter = try coerce(.{ .string = "{}" }, .json);
    try std.testing.expect(typed_parameter == .string);
    try std.testing.expectError(error.SqlTypeMismatch, bindLiteral(arena.allocator(), .{ .string = "not JSON" }, .json));
    try std.testing.expect((try bindLiteral(arena.allocator(), .{ .string = "null" }, .json)) == .null);
    try std.testing.expect((try bindLiteral(arena.allocator(), .null, .json)) == .null);
    const nested_null = try bindLiteral(arena.allocator(), .{ .string = "{\"n\":null}" }, .json);
    try std.testing.expect(nested_null.object.get("n").? == .null);
    try std.testing.expectError(error.SqlProgramLimitExceeded, bindLiteral(arena.allocator(), .{ .string = z17RepeatString("[", 65) ++ "0" ++ z17RepeatString("]", 65) }, .json));
}

test "SQL JSON literal coercion propagates allocation failure without rewriting errors" {
    const Check = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            _ = try bindLiteral(arena.allocator(), .{ .string = "{\"a\":[1,2,3],\"s\":\"preserved\"}" }, .json);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "SQL binding parses identical JSON literals once and owns them after compiled SQL is released" {
    var fake: FakeBackend = .{};
    var compiled = try compiler.compile(
        std.testing.allocator,
        "INSERT INTO things (_id,payload) VALUES ('a','{\"n\":9007199254740993}'),('b','{\"n\":9007199254740993}')",
        .{},
    );
    var result = try describe(std.testing.allocator, fake.backend(), &compiled, &.{});
    compiled.deinit();
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.binding.json_literals.count());
    const tree = result.binding.json_literals.get("{\"n\":9007199254740993}").?;
    try std.testing.expectEqualStrings("9007199254740993", tree.object.get("n").?.number_string);
    var keys = result.binding.json_literals.keyIterator();
    try std.testing.expectEqualStrings("{\"n\":9007199254740993}", keys.next().?.*);
}

test "SQL binding rejects generated column writes even when a predicate is provably empty" {
    const cases = [_][]const u8{
        "INSERT INTO things (_id,derived) VALUES ('x',1)",
        "INSERT INTO things (_id,derived) VALUES ($1,$2)",
        "UPDATE things SET derived=1",
        "UPDATE things SET derived=$1 WHERE _id IS NULL",
    };
    for (cases) |source| {
        var fake: FakeBackend = .{};
        var compiled = try compiler.compile(std.testing.allocator, source, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.SqlGeneratedColumnWrite, describe(std.testing.allocator, fake.backend(), &compiled, &.{}));
    }
    var fake: FakeBackend = .{};
    var read = try compiler.compile(std.testing.allocator, "SELECT * FROM things", .{});
    defer read.deinit();
    var result = try describe(std.testing.allocator, fake.backend(), &read, &.{});
    defer result.deinit();
    try std.testing.expectEqualStrings("derived", result.binding.columns[result.binding.columns.len - 1].name);
}

test "SQL timestamp coercion preserves signed epoch precision and canonical offsets" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const expected = "2026-01-01T00:00:00.123456789Z";
    const ns = @import("../datetime.zig").parseDateTimeToNs(expected).?;
    for ([_]Json{ .{ .integer = @intCast(ns) }, .{ .number_string = try std.fmt.allocPrint(a, "{d}", .{ns}) }, .{ .string = "2026-01-01T01:00:00.123456789+01:00" } }) |raw| {
        try std.testing.expectEqualStrings(expected, (try coerceAlloc(a, raw, .datetime)).string);
    }
    try std.testing.expect((try coerceAlloc(a, .null, .datetime)) == .null);
    for ([_]Json{ .{ .integer = -1 }, .{ .number_string = "-1" }, .{ .string = "1969-12-31T23:59:59.999999999Z" } }) |raw| {
        try std.testing.expectEqualStrings("1969-12-31T23:59:59.999999999Z", (try coerceAlloc(a, raw, .datetime)).string);
    }
    try std.testing.expectError(error.InvalidSqlDateTime, coerceAlloc(a, .{ .number_string = "170141183460469231731687303715884105727" }, .datetime));
}

fn z17RepeatString(comptime bytes: []const u8, comptime repetitions: usize) *const [bytes.len * repetitions:0]u8 {
    const result = comptime blk: {
        @setEvalBranchQuota(@intCast(@min(std.math.maxInt(u32), 100000 +| (repetitions *| 16))));
        var repeated: [bytes.len * repetitions:0]u8 = undefined;
        for (0..repetitions) |i| @memcpy(repeated[i * bytes.len ..][0..bytes.len], bytes);
        repeated[bytes.len * repetitions] = 0;
        break :blk repeated;
    };
    return &result;
}
