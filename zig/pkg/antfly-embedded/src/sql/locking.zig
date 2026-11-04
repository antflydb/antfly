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

//! LockRows retains physical row identity through selection and ordering.
//! Native providers own waiting, current-version/RLS checks and transaction
//! lifetime. SQL applies OFFSET/LIMIT and volatile projections only after the
//! current row was successfully locked and its predicate was rechecked.
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const compiler = @import("compiler.zig");
const describe = @import("describe.zig");
const relations = @import("relation_binding.zig");
const runtime = @import("runtime.zig");
const scalar = @import("scalar.zig");
const contracts = @import("../storage/pessimistic_lock_contract.zig");
const A = std.mem.Allocator;

pub const Identity = struct { key: []const u8, version: u64, digest: ?[32]u8 = null };
pub const Provider = struct {
    ptr: *anyopaque,
    /// Pure permission checks run for every target before the first grant.
    authorize_table: ?*const fn (*anyopaque, A, catalog.Table, contracts.TableMode) anyerror!void = null,
    authorize_rows: ?*const fn (*anyopaque, A, catalog.Table) anyerror!void = null,
    require_explicit_transaction: ?*const fn (*anyopaque) anyerror!void = null,
    table: *const fn (*anyopaque, A, catalog.Table, contracts.TableMode, ast.LockWait) anyerror!void,
    /// Returned values belong to the supplied scratch allocator. null means a
    /// skipped contender, deleted row, or row no longer visible under RLS.
    /// Snapshot isolation rejects a changed version with serialization failure;
    /// READ COMMITTED returns the independently authorized current row.
    row: *const fn (*anyopaque, A, catalog.Table, Identity, catalog.Row, contracts.RowMode, ast.LockWait, []const []const u8) anyerror!?catalog.Row,
};
pub const Plan = union(enum) { rows: Bound, tables: Tables };
pub const Tables = struct { targets: []const catalog.Table, mode: contracts.TableMode, wait: ast.LockWait };
pub fn bindTables(a: A, backend: catalog.Backend, input: ast.TableLock) !describe.BoundStatement {
    const provider = backend.locking_provider orelse return error.UnsupportedSqlExecution;
    const authorize = provider.authorize_table orelse return error.UnsupportedSqlExecution;
    const targets = try a.alloc(catalog.Table, input.targets.len);
    for (input.targets, targets) |target, *table| {
        table.* = try backend.vtable.resolve(backend.ptr, a, target.name, .read);
        if (table.external_base_source != null) return error.ExternalLakeReadOnly;
        try authorize(provider.ptr, a, table.*, input.mode);
    }
    const plan = try a.create(Plan);
    plan.* = .{ .tables = .{ .targets = targets, .mode = input.mode, .wait = input.wait } };
    return .{ .table = null, .action = .read_write, .columns = &.{}, .parameter_types = &.{}, .json_literals = .empty, .locking = plan };
}
pub fn executeTables(context: runtime.Context, plan: *const Tables) !runtime.Output {
    const provider = context.backend.locking_provider orelse return error.UnsupportedSqlExecution;
    try (provider.require_explicit_transaction orelse return error.UnsupportedSqlExecution)(provider.ptr);
    for (plan.targets) |table| try provider.table(provider.ptr, context.arena, table, plan.mode, plan.wait);
    return .{ .command_tag = "LOCK TABLE" };
}

pub const Bound = struct {
    table: catalog.Table,
    read: describe.BoundStatement,
    statement: ast.Select,
    candidates: describe.BoundStatement,
    candidate_query: ast.Select,
    fields: []const []const u8,
    candidate_fields: []const []const u8,
    mode: contracts.RowMode,
    wait: ast.LockWait,
};

fn scopeColumns(a: A, table: catalog.Table, qualifier: []const u8, aliased: bool) ![]const relations.Column {
    const virtual = table.virtualColumns();
    const result = try a.alloc(relations.Column, table.columns.len + virtual.len);
    for (table.columns, result[0..table.columns.len]) |column, *out| out.* = .{ .name = column.name, .internal = column.name, .qualifier = qualifier, .scope = if (aliased) null else table.scope, .type = column.type, .element_type = column.element_type, .temporal_kind = column.temporal_kind, .numeric_modifier = column.numeric_modifier, .enum_view = column.enum_view, .domain_view = scalar.domainViewOf(column), .nullable = column.nullable };
    for (virtual, result[table.columns.len..]) |column, *out| out.* = .{ .name = column.name, .internal = column.name, .qualifier = qualifier, .scope = if (aliased) null else table.scope, .type = column.type, .element_type = column.element_type, .nullable = column.nullable };
    return result;
}
fn columnNode(a: A, qualifier: []const u8, name: []const u8) !*const ast.Scalar {
    const value = try a.create(ast.Scalar);
    value.* = .{ .column = try std.fmt.allocPrint(a, "{s}\x00{s}", .{ qualifier, name }) };
    return value;
}
fn appendField(a: A, fields: *std.ArrayList([]const u8), name: []const u8) !void {
    for (fields.items) |previous| if (std.mem.eql(u8, previous, name)) return;
    try fields.append(a, name);
}

pub fn bind(a: A, backend: catalog.Backend, input: *const compiler.Compiled, hints: []const ?ast.ColumnType) !describe.BoundStatement {
    if (backend.locking_provider == null or input.statement != .select) return error.UnsupportedSqlExecution;
    var statement = try @import("order_aliases.zig").normalize(a, input.statement.select);
    const clauses = statement.locking orelse return error.InvalidSqlBackendResponse;
    if (statement.count_all or statement.distinct != null or statement.group_by.len != 0 or statement.having != null or statement.set_operation != null or statement.values_arms.len != 0 or statement.ctes.len != 0 or @import("aggregate_binding.zig").accepts(statement) or @import("window_binding.zig").accepts(statement)) return error.UnsupportedSqlShape;
    const reference: @FieldType(ast.Relation, "table") = if (statement.source) |source| switch (source.*) {
        .table => |table| table,
        else => return error.UnsupportedSqlShape,
    } else .{ .name = statement.table orelse return error.UnsupportedSqlShape };
    if (reference.search != null or reference.prepared_rows or reference.mutation_target) return error.UnsupportedSqlShape;
    const qualifier = reference.alias orelse reference.name.table;
    var mode: contracts.RowMode = .key_share;
    var wait: ast.LockWait = .block;
    for (clauses.clauses) |clause| {
        for (clause.relations) |name| if (!std.mem.eql(u8, name, qualifier)) return error.UndefinedTable;
        if (@backingInt(clause.mode) > @backingInt(mode)) mode = clause.mode;
        if (clause.wait == .nowait or (wait != .nowait and clause.wait == .skip_locked)) wait = clause.wait;
    }
    var table = try backend.vtable.resolve(backend.ptr, a, reference.name, .read_write);
    table.row_policy_purpose = .update_select;
    if (backend.locking_provider.?.authorize_rows) |authorize_rows| try authorize_rows(backend.locking_provider.?.ptr, a, table);
    if (table.external_base_source != null) return error.ExternalLakeReadOnly;
    const scope = try scopeColumns(a, table, qualifier, reference.alias != null);
    statement.locking = null;
    statement.source = null;
    statement.table = reference.name;
    var projection_name = reference.name;
    projection_name.table = qualifier;
    statement.columns = try relations.normalizeTargetProjection(a, backend, table, projection_name, reference.alias != null, statement.columns);
    const projections = try a.dupe(ast.Projection, statement.columns);
    for (projections) |*projection| {
        if (projection.expression) |expression| projection.expression = try relations.lowerBoundExpression(a, scope, expression) else if (!projection.wildcard) projection.field = (try relations.lookupColumn(scope, projection.field)).internal;
    }
    statement.columns = projections;
    if (statement.predicate) |predicate| {
        const value = try a.create(ast.Predicate);
        value.* = .{ .scalar = try relations.lowerBoundExpression(a, scope, try @import("bound_scalars.zig").predicateScalar(a, table, try relations.lowerBoundPredicate(a, scope, predicate))) };
        statement.predicate = value;
    }
    const orders = try a.dupe(ast.Order, statement.order_by);
    for (orders) |*order| {
        if (order.position) |position| {
            if (position == 0 or position > projections.len) return error.UndefinedColumn;
            order.expression = projections[position - 1].expression orelse node: {
                const value = try a.create(ast.Scalar);
                value.* = .{ .column = projections[position - 1].field };
                break :node value;
            };
            order.position = null;
        } else if (order.expression) |expression| order.expression = try relations.lowerBoundExpression(a, scope, expression);
    }
    for (orders) |*order| if (order.expression != null) {
        order.field = "";
    };
    statement.order_by = orders;
    var compiled = input.*;
    compiled.uses_row_locks = false;
    compiled.statement = .{ .select = statement };
    var adapter: relations.ResolveAdapter = .{ .backend = backend, .table = table };
    const read = try describe.bindInternal(a, adapter.iface(), &compiled, hints);
    // This node owns one physical base source. Nested relation/subquery shapes
    // must retain their own LockRows provenance rather than lose a cold clause.
    if (read.relation != null or read.aggregate != null or read.window != null) return error.UnsupportedSqlShape;
    var fields: std.ArrayList([]const u8) = .empty;
    for (read.scalars.required) |ordinal| try appendField(a, &fields, read.scalars.columns[ordinal].name);
    for (statement.columns) |projection| if (projection.expression == null) try appendField(a, &fields, projection.field);
    for (read.order_keys) |key| if (key.source == .column) try appendField(a, &fields, key.source.column.name);
    const visible_fields = try a.dupe([]const u8, fields.items);
    try appendField(a, &fields, "_id");
    try appendField(a, &fields, @import("joined_mutation.zig").metadata_fields[0]);
    try appendField(a, &fields, @import("joined_mutation.zig").metadata_fields[1]);
    const candidate_projections = try a.alloc(ast.Projection, fields.items.len);
    for (fields.items, candidate_projections) |field, *projection| projection.* = .{ .expression = try columnNode(a, qualifier, field) };
    const source = try a.create(ast.Relation);
    source.* = .{ .table = reference };
    source.table.mutation_target = true;
    var candidates = statement;
    candidates.source = source;
    candidates.table = null;
    candidates.columns = candidate_projections;
    candidates.limit = null;
    candidates.offset = null;
    candidates.internal_projection = true;
    candidates.scalar_cardinality_limit = false;
    compiled.statement = .{ .select = candidates };
    // Bind candidates against the same read/write-authorized table. Resolving
    // it again as a plain SELECT would lose UPDATE USING policy admission.
    var candidate_target: relations.TargetResolveAdapter = .{ .backend = backend, .table = table, .name = reference.name };
    const candidate_binding = try describe.bindInternal(a, candidate_target.iface(), &compiled, read.parameter_types);
    const plan = try a.create(Plan);
    plan.* = .{ .rows = .{ .table = table, .read = read, .statement = statement, .candidates = candidate_binding, .candidate_query = candidates, .fields = visible_fields, .candidate_fields = fields.items, .mode = mode, .wait = wait } };
    var result = read;
    result.locking = plan;
    return result;
}

pub fn execute(request: runtime.Context, plan: *const Bound) !runtime.Output {
    const provider = request.backend.locking_provider orelse return error.UnsupportedSqlExecution;
    try provider.table(provider.ptr, request.arena, plan.table, .row_share, .block);
    var context = request;
    context.binding = plan.read;
    const limit = plan.statement.capRows(try context.count(plan.statement.limit, context.limits.result_rows));
    var offset = try context.offsetCount(plan.statement.offset);
    if (limit > context.limits.result_rows or offset > context.limits.scan_rows) return error.SqlProgramLimitExceeded;
    if (limit == 0) return .{ .columns = plan.read.columns, .command_tag = "SELECT" };
    var candidates = context;
    candidates.binding = plan.candidates;
    candidates.limits.result_rows = context.limits.scan_rows;
    const cursor = try @import("relation_runtime.zig").openQueryCursor(candidates);
    defer cursor.close(cursor.ptr);
    var scratch = std.heap.ArenaAllocator.init(context.alloc);
    defer scratch.deinit();
    var output_rows: std.ArrayList([]const std.json.Value) = .empty;
    var output_nulls: std.ArrayList([]const bool) = .empty;
    var output_native: std.ArrayList([]const scalar.Datum) = .empty;
    const candidate_layout = try catalog.Row.TypedLayout.init(context.arena, plan.candidate_fields);
    var delivered: usize = 0;
    while (delivered < limit) {
        try context.checkpoint();
        _ = scratch.reset(.retain_capacity);
        const a = scratch.allocator();
        const page = try cursor.next(cursor.ptr, a, 1);
        defer page.deinit();
        if (page.rows.len == 0) {
            if (page.after == null) break;
            continue;
        }
        if (page.rows.len != 1) return error.InvalidSqlBackendResponse;
        const cells = (page.rows[0].typed_cells orelse return error.InvalidSqlBackendResponse).values;
        if (cells.len != plan.candidate_fields.len) return error.InvalidSqlBackendResponse;
        var identity: Identity = .{ .key = "", .version = 0 };
        for (plan.candidate_fields, cells) |name, cell| {
            if (std.mem.eql(u8, name, "_id")) {
                if (cell.sql_null or cell.value != .string) return error.InvalidSqlBackendResponse;
                identity.key = cell.value.string;
            } else if (std.mem.eql(u8, name, @import("joined_mutation.zig").metadata_fields[0])) {
                if (cell.sql_null or cell.value != .string) return error.InvalidSqlBackendResponse;
                identity.version = std.fmt.parseInt(u64, cell.value.string, 10) catch return error.InvalidSqlBackendResponse;
            } else if (std.mem.eql(u8, name, @import("joined_mutation.zig").metadata_fields[1])) {
                if (cell.sql_null or cell.value != .string) return error.InvalidSqlBackendResponse;
                if (cell.value.string.len != 0) {
                    var digest: [32]u8 = undefined;
                    _ = std.fmt.hexToBytes(&digest, cell.value.string) catch return error.InvalidSqlBackendResponse;
                    identity.digest = digest;
                }
            }
        }
        if (identity.key.len == 0) return error.InvalidSqlBackendResponse;
        const captured: catalog.Row = .{ .id = identity.key, .version = identity.version, .expected_content_digest = identity.digest, .value = .null, .typed_cells = .{ .layout = candidate_layout, .values = cells } };
        const current = (try provider.row(provider.ptr, a, plan.table, identity, captured, plan.mode, plan.wait, plan.fields)) orelse continue;
        const current_cells = try plan.read.scalars.cells(a, current);
        if (!try context.matchesRow(a, current_cells)) continue;
        if (offset != 0) {
            offset -= 1;
            continue;
        }
        const fields = try a.alloc([]const u8, plan.statement.columns.len);
        for (plan.statement.columns, fields) |projection, *name| name.* = projection.field;
        const values = try context.projectValues(a, current, fields, current_cells);
        if (context.sink) |sink| try sink.append(sink.ptr, values) else {
            const row = try context.resultRow(values, plan.read.columns);
            if (row.native) |native_row| try output_native.append(context.arena, native_row) else {
                try output_rows.append(context.arena, row.json);
                try output_nulls.append(context.arena, row.nulls);
            }
        }
        delivered += 1;
    }
    return .{ .columns = plan.read.columns, .rows = output_rows.items, .sql_nulls = output_nulls.items, .native_rows = if (context.limits.native_result and context.sink == null) output_native.items else null, .command_tag = "SELECT" };
}
