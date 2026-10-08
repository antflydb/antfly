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

//! Native-fenced primary and unique arbiters with compiled old/excluded expressions.
//! Every observed row (including a skipped row) remains a native commit fence.
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const scalar = @import("scalar.zig");
const decision_eval = @import("decision_eval.zig");

pub const Bound = struct {
    pub const Deferred = struct { query: *const ast.Select, binding: *const @import("describe.zig").BoundStatement };
    columns: []const scalar.Column,
    row_width: usize,
    assignments: []const ?scalar.Program,
    // Dense target ordinals eliminate assignment-name scans for every row.
    column_assignments: []const ?usize,
    deferred: []const ?Deferred = &.{},
    predicate: ?scalar.Program,
    arbiter_conditions: []const catalog.Condition = &.{},
    arbiter_expressions: []const catalog.ConflictExpression = &.{},
};

pub fn bind(alloc: std.mem.Allocator, backend: catalog.Backend, table: catalog.Table, name: ast.Name, aliased: bool, clause: ast.Conflict, parameters: []?ast.ColumnType, capture_types: []const scalar.Type) !Bound {
    if (clause.constraint_name) |constraint_name| {
        if (clause.columns.len != 0 or clause.expressions.len != 0 or clause.arbiter_predicate != null) return error.InvalidSqlBackendResponse;
        const constraint = for (table.constraints) |value| {
            if (std.mem.eql(u8, value.name, constraint_name)) break value;
        } else return error.SqlConstraintNotFound;
        if (constraint.kind != .unique) return error.WrongConflictConstraintKind;
        // PostgreSQL rejects deferrable arbiters during execution, after
        // proposed-row defaults. Native generation binding owns that check.
    }
    if ((clause.capture_count != 0 or clause.deferred_count != 0) and (!backend.atomic_statement_read_set or backend.vtable.open_statement == null)) return error.SqlRangeTrackingRequired;
    if (clause.deferred_count != 0 and !backend.dynamic_statement_read_set) return error.SqlStatementSnapshotRequired;
    if (capture_types.len != clause.capture_count) return error.InvalidSqlBackendResponse;
    // Secondary unique arbiters require a native unique-key reservation, not
    // a scan of a possibly partial index. Refuse them until that authority is
    // exposed by the native coordinator.
    if (!primary(clause)) {
        if (backend.vtable.resolve_conflict_owners == null) return error.UnsupportedSqlShape;
        for (clause.columns) |column| _ = try table.column(column);
    }
    const arbiter_conditions = if (clause.arbiter_predicate) |predicate| try bindArbiterPredicate(alloc, table, predicate) else &.{};
    const arbiter_expressions = try alloc.alloc(catalog.ConflictExpression, clause.expressions.len);
    if (clause.expressions.len != 0) {
        const source_columns = try alloc.alloc(scalar.Column, table.columns.len);
        for (table.columns, source_columns) |column, *out| out.* = .{ .name = column.name, .type = column.type, .nullable = column.nullable };
        for (clause.expressions, arbiter_expressions) |expression, *out| {
            const lowered = try @import("schema_expression.zig").lowerColumns(alloc, source_columns, expression, null);
            out.* = .{ .json = try std.json.Stringify.valueAlloc(alloc, lowered.expression, .{}), .result_type = lowered.type };
        }
    }
    const count = table.columns.len + 1;
    const columns = try alloc.alloc(scalar.Column, count * 3 + clause.capture_count);
    for (0..count) |i| {
        const column = if (i == table.columns.len) try table.column("_id") else table.columns[i];
        columns[i] = .{ .name = column.name, .type = column.type, .element_type = column.element_type, .nullable = column.nullable };
        columns[count + i] = .{ .name = try std.fmt.allocPrint(alloc, "{s}\x00{s}", .{ name.table, column.name }), .type = column.type, .element_type = column.element_type, .nullable = column.nullable };
        if (!aliased) {
            const scope = table.scope orelse if (name.namespace) |namespace| catalog.Table.Scope{ .database = name.database orelse "", .namespace = namespace, .name = name.table, .revision = 0 } else null;
            if (scope) |logical| {
                const aliases = try alloc.alloc([]const u8, if (logical.database.len == 0) 1 else 2);
                aliases[0] = try std.fmt.allocPrint(alloc, "{s}\x00{s}\x00{s}", .{ logical.namespace, logical.name, column.name });
                if (aliases.len == 2) aliases[1] = try std.fmt.allocPrint(alloc, "{s}\x00{s}\x00{s}\x00{s}", .{ logical.database, logical.namespace, logical.name, column.name });
                columns[count + i].aliases = aliases;
            }
        }
        columns[count * 2 + i] = .{ .name = try std.fmt.allocPrint(alloc, "excluded\x00{s}", .{column.name}), .type = column.type, .element_type = column.element_type, .nullable = column.nullable };
    }
    for (columns[count * 3 ..], capture_types, 0..) |*column, descriptor, ordinal| {
        column.* = .{ .name = try std.fmt.allocPrint(alloc, "$conflict_capture_{d}", .{ordinal}), .type = descriptor.kind orelse return error.InvalidSqlBackendResponse, .element_type = descriptor.element_type, .nullable = true };
    }
    var pass: usize = 0;
    const column_assignments = try alloc.alloc(?usize, table.columns.len);
    @memset(column_assignments, null);
    for (clause.assignments, 0..) |assignment, assignment_index| {
        const target = try table.column(assignment.field);
        if (target.generated and !assignment.use_default) return error.SqlGeneratedColumnWrite;
        if (std.mem.eql(u8, target.name, "_id")) return error.UnsupportedSqlShape;
        const ordinal = for (table.columns, 0..) |column, index| {
            if (std.mem.eql(u8, column.name, target.name)) break index;
        } else return error.InvalidSqlBackendResponse;
        if (column_assignments[ordinal] != null) return error.DuplicateSqlColumn;
        column_assignments[ordinal] = assignment_index;
    }
    while (true) : (pass += 1) {
        if (pass > parameters.len + 1) return error.ConflictingSqlParameterTypes;
        var changed = false;
        for (clause.assignments) |assignment| {
            const column = try table.column(assignment.field);
            if (assignment.use_default) continue;
            const expression = assignment.expression orelse return error.InvalidSqlBackendResponse;
            if (assignment.deferred_scalar) continue;
            if (assignment.capture_ordinal) |ordinal| {
                if (ordinal + assignment.capture_span > clause.capture_count) return error.InvalidSqlBackendResponse;
                if (assignment.capture_expression == null) continue;
            }
            const expected: scalar.Type = .{ .kind = column.type, .element_type = column.element_type };
            const root = try scalar.assignmentExpression(alloc, assignment.capture_expression orelse expression, expected);
            changed = (if (backend.parameter_invocation) |owner|
                try owner.infer(alloc, root, columns, parameters, expected, .{ .assignment = true })
            else if (column.type == .array and parameters.len == 0)
                try scalar.inferTypedParametersExpected(alloc, root, columns, &.{}, expected, .{ .assignment = true })
            else
                try scalar.inferParameters(alloc, root, columns, parameters, column.type, .{ .assignment = true })) or changed;
        }
        if (clause.predicate) |expression| changed = try scalar.inferParameters(alloc, expression, columns, parameters, .boolean, .{ .invocation = backend.parameter_invocation }) or changed;
        if (!changed) break;
    }
    const assignments = try alloc.alloc(?scalar.Program, clause.assignments.len);
    const deferred = try alloc.alloc(?Bound.Deferred, clause.assignments.len);
    for (clause.assignments, assignments, deferred) |assignment, *program, *later| {
        later.* = null;
        if (assignment.use_default) {
            program.* = null;
            continue;
        }
        if (assignment.deferred_scalar) {
            program.* = null;
            const expression = assignment.expression orelse return error.InvalidSqlBackendResponse;
            if (expression.* != .call or expression.call.subquery == null or !std.mem.eql(u8, expression.call.name, "$scalar")) return error.InvalidSqlBackendResponse;
            const query = expression.call.subquery.?;
            const compiled: @import("compiler.zig").Compiled = .{ .arena = undefined, .statement = .{ .select = query.* }, .parameter_count = @intCast(parameters.len) };
            const binding = try alloc.create(@import("describe.zig").BoundStatement);
            binding.* = try @import("describe.zig").bind(alloc, backend, &compiled, parameters);
            if (binding.columns.len != 1) return error.InvalidSqlParameters;
            const target = try table.column(assignment.field);
            if (binding.columns[0].type != target.type and !(binding.columns[0].type == .integer and target.type == .number)) return error.SqlTypeMismatch;
            later.* = .{ .query = query, .binding = binding };
            continue;
        }
        if (assignment.capture_ordinal != null and assignment.capture_expression == null) {
            program.* = null;
        } else {
            const column = try table.column(assignment.field);
            const expected: scalar.Type = .{ .kind = column.type, .element_type = column.element_type };
            const root = try scalar.assignmentExpression(alloc, assignment.capture_expression orelse assignment.expression.?, expected);
            program.* = if (backend.parameter_invocation) |owner|
                try scalar.bindTypedExpectedWithSettings(alloc, root, columns, owner.descriptors, expected, .{ .invocation = owner, .assignment = true }, backend.settings_view)
            else if (column.type == .array and parameters.len == 0)
                try scalar.bindTypedExpectedWithSettings(alloc, root, columns, &.{}, expected, .{ .assignment = true }, backend.settings_view)
            else
                try scalar.bindExpectedWithSettings(alloc, root, columns, parameters, column.type, .{ .assignment = true }, backend.settings_view);
        }
    }
    return .{ .columns = columns, .row_width = count, .assignments = assignments, .column_assignments = column_assignments, .deferred = deferred, .predicate = if (clause.predicate) |expression| try scalar.bindExpectedWithSettings(alloc, expression, columns, parameters, .boolean, .{ .invocation = backend.parameter_invocation }, backend.settings_view) else null, .arbiter_conditions = arbiter_conditions, .arbiter_expressions = arbiter_expressions };
}

fn bindArbiterPredicate(alloc: std.mem.Allocator, table: catalog.Table, expression: *const ast.Scalar) ![]const catalog.Condition {
    var conditions: std.ArrayList(catalog.Condition) = .empty;
    try appendArbiterConditions(alloc, table, expression, &conditions);
    if (conditions.items.len == 0 or conditions.items.len > 32) return error.UnsupportedSqlShape;
    return conditions.toOwnedSlice(alloc);
}

fn appendArbiterConditions(alloc: std.mem.Allocator, table: catalog.Table, expression: *const ast.Scalar, output: *std.ArrayList(catalog.Condition)) anyerror!void {
    if (expression.* == .binary and expression.binary.op == .@"and") {
        try appendArbiterConditions(alloc, table, expression.binary.left, output);
        try appendArbiterConditions(alloc, table, expression.binary.right, output);
        return;
    }
    if (expression.* == .unary and (expression.unary.op == .is_null or expression.unary.op == .is_not_null)) {
        if (expression.unary.operand.* != .column) return error.UnsupportedSqlShape;
        const column = try table.column(expression.unary.operand.column);
        try output.append(alloc, .{ .column = column.name, .op = if (expression.unary.op == .is_null) .is_null else .is_not_null });
        return;
    }
    if (expression.* != .binary) return error.UnsupportedSqlShape;
    const comparison = expression.binary;
    if (comparison.op == .@"and") return error.InvalidSqlBackendResponse;
    const op = switch (comparison.op) {
        .eq => catalog.Condition.Op.eq,
        .neq => .neq,
        .lt => .lt,
        .lte => .lte,
        .gt => .gt,
        .gte => .gte,
        else => return error.UnsupportedSqlShape,
    };
    const column_expression, const literal_expression, const reverse = if (comparison.left.* == .column and comparison.right.* == .literal)
        .{ comparison.left, comparison.right, false }
    else if (comparison.right.* == .column and comparison.left.* == .literal)
        .{ comparison.right, comparison.left, true }
    else
        return error.UnsupportedSqlShape;
    const column = try table.column(column_expression.column);
    const literal = literal_expression.literal;
    const raw: std.json.Value = switch (literal) {
        .null => return error.UnsupportedSqlShape,
        .boolean => |value| .{ .bool = value },
        .integer => |value| .{ .integer = value },
        .number => |value| .{ .float = value },
        .numeric => |value| .{ .number_string = value },
        .string => |value| .{ .string = value },
        .parameter => return error.InvalidSqlParameters,
    };
    const value = try @import("describe.zig").coerce(raw, column.type);
    try output.append(alloc, .{ .column = column.name, .op = if (reverse) invert(op) else op, .value = value });
}

fn invert(op: catalog.Condition.Op) catalog.Condition.Op {
    return switch (op) {
        .lt => .gt,
        .lte => .gte,
        .gt => .lt,
        .gte => .lte,
        else => op,
    };
}

pub fn primary(clause: ast.Conflict) bool {
    return clause.constraint_name == null and clause.expressions.len == 0 and clause.columns.len == 1 and std.mem.eql(u8, clause.columns[0], "_id");
}

pub fn allowsDuplicateKeys(clause: ast.Conflict) bool {
    return clause.assignments.len == 0 and (primary(clause) or (clause.constraint_name == null and clause.columns.len == 0 and clause.expressions.len == 0));
}

/// A retained point snapshot plus an atomic version predicate is optimistic
/// concurrency control, not a read-then-overwrite. A racing insert/update is a
/// definite serialization conflict, never an automatically replayed mutation.
pub fn resolve(context: anytype, table: catalog.Table, clause: ast.Conflict, binding: Bound, proposed: []const catalog.Mutation, captured: []const []const scalar.Datum) ![]const catalog.Mutation {
    if (clause.capture_count != 0 and captured.len != proposed.len) return error.InvalidSqlBackendResponse;
    if (!context.backend.predicate_only_mutations) return error.UnsupportedSqlExecution;
    if (table.storage_mode != .relational) return error.UnsupportedSqlExecution;
    const prepare = context.backend.vtable.prepare_mutations orelse return error.UnsupportedSqlExecution;
    const normalized = try prepare(context.backend.ptr, context.arena, table, proposed);
    if (normalized.len != proposed.len) return error.InvalidSqlBackendResponse;
    const owners = if (!primary(clause)) try (context.backend.vtable.resolve_conflict_owners orelse return error.UnsupportedSqlExecution)(context.backend.ptr, context.arena, table, .{ .columns = clause.columns, .expressions = binding.arbiter_expressions, .conditions = binding.arbiter_conditions, .constraint_name = clause.constraint_name }, normalized) else null;
    if (owners) |items| if (items.len != normalized.len) return error.InvalidSqlBackendResponse;
    if (clause.constraint_name == null and clause.columns.len == 0 and clause.expressions.len == 0) {
        for (proposed, normalized) |original, value| {
            if (!std.mem.eql(u8, original.key, value.key) or value.row == null or value.expected_version != 0) return error.InvalidSqlBackendResponse;
        }
        return resolveAny(context, table, binding, normalized, owners.?);
    }
    return resolvePrepared(context, table, clause, binding, proposed, normalized, owners, captured);
}

fn resolvePrepared(context: anytype, table: catalog.Table, clause: ast.Conflict, binding: Bound, proposed: []const catalog.Mutation, normalized: []const catalog.Mutation, owners: ?[]const catalog.ConflictOwner, captured: []const []const scalar.Datum) ![]const catalog.Mutation {
    if (binding.column_assignments.len != table.columns.len) return error.InvalidSqlBackendResponse;
    if (binding.deferred.len != 0 and binding.deferred.len != clause.assignments.len) return error.InvalidSqlBackendResponse;
    const buffer = try context.arena.alloc(catalog.Mutation, normalized.len);
    const captured_buffer = try context.arena.alloc([]const scalar.Datum, normalized.len);
    const deferred_cache = try context.arena.alloc(?scalar.Datum, clause.assignments.len);
    @memset(deferred_cache, null);
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    var owner_by_key: std.StringHashMapUnmanaged(catalog.ConflictOwner) = .empty;
    var count: usize = 0;
    for (proposed, normalized, 0..) |original, value, position| {
        var mutation = value;
        if (!std.mem.eql(u8, mutation.key, original.key) or mutation.row == null or mutation.expected_version != 0) return error.InvalidSqlBackendResponse;
        const owner = if (owners) |items| items[position] else null;
        if (owner) |item| {
            if (item.guard == null or (item.key != null and item.identity == null)) return error.InvalidSqlBackendResponse;
        }
        const identity = if (owner) |item| item.identity orelse mutation.key else mutation.key;
        if ((try seen.getOrPut(context.arena, identity)).found_existing) {
            if (clause.assignments.len != 0) return error.DuplicateSqlRow;
            continue;
        }
        if (owner) |item| {
            mutation.conflict_guard = item.guard;
            try owner_by_key.put(context.arena, mutation.key, item);
        }
        buffer[count] = mutation;
        captured_buffer[count] = if (captured.len == 0) &.{} else captured[position];
        count += 1;
    }
    const result = buffer[0..count];
    var fields: std.ArrayList([]const u8) = .empty;
    if (clause.assignments.len != 0) for (table.columns, 0..) |column, ordinal| {
        // Preserve ordinary columns for replacement; generated columns are
        // recomputed natively and fetched only if old-row expressions need one.
        const replaced = binding.column_assignments[ordinal] != null;
        var needed = !column.generated and !replaced;
        const width = binding.row_width;
        for (binding.assignments) |program| if (program) |bound_program| for (bound_program.required_columns) |required| {
            if (required < width * 2 and required % width == ordinal) needed = true;
        };
        if (binding.predicate) |program| for (program.required_columns) |required| {
            if (required < width * 2 and required % width == ordinal) needed = true;
        };
        if (needed) try fields.append(context.arena, column.path);
    };
    // The conflict-resolution scan-page budget belongs to the whole mutation
    // batch, not each conflicted key. Otherwise a bounded batch can multiply
    // one expensive point cursor by the mutation-row limit before commit.
    var has_decisions = if (binding.predicate) |*program| decision_eval.hasExternal(program) else false;
    for (binding.assignments) |optional| if (optional) |*program| {
        has_decisions = has_decisions or decision_eval.hasExternal(program);
    };
    var decision_arena = std.heap.ArenaAllocator.init(context.alloc);
    defer decision_arena.deinit();
    var pending: std.ArrayList(DecisionConflictRow) = .empty;
    var page_budget: decision_eval.PageBudget = .{ .row_limit = context.limits.page_rows, .byte_limit = context.limits.page_bytes };
    var point_pages: usize = 0;
    for (result, captured_buffer[0..count]) |*mutation, captured_row| {
        try context.checkpoint();
        const conflict_owner = owner_by_key.get(mutation.key);
        if (conflict_owner) |owner| if (owner.key == null) continue;
        const lookup_key = if (conflict_owner) |owner| owner.key.? else mutation.key;
        if (point_pages >= context.limits.scan_pages) return error.SqlProgramLimitExceeded;
        // A point read can carry a large native page/continuation even though
        // only its fenced row image survives this iteration. Reclaim that
        // scratch before opening the next owner in a large conflict batch.
        var cursor_arena: std.heap.ArenaAllocator = .init(context.alloc);
        defer cursor_arena.deinit();
        var page_arena: std.heap.ArenaAllocator = .init(context.alloc);
        defer page_arena.deinit();
        const page_alloc = page_arena.allocator();
        const open = context.backend.vtable.open_scan orelse return error.SqlStatementSnapshotRequired;
        const cursor = (try open(context.backend.ptr, cursor_arena.allocator(), table, .{ .fields = fields.items, .primary_key = lookup_key, .limit = 1, .include_primary_digest = true })) orelse return error.SqlStatementSnapshotRequired;
        defer cursor.close(cursor.ptr);
        var page = try cursor.next(cursor.ptr, page_alloc, 2);
        defer page.deinit();
        point_pages += 1;
        // Native point ranges may yield an empty progress page while skipping
        // expired rows; only exhaustion proves absence. A row-filled page can
        // carry continuation even though the exact key already resolves.
        while (page.rows.len == 0 and page.after != null) {
            try context.checkpoint();
            if (point_pages >= context.limits.scan_pages) return error.SqlProgramLimitExceeded;
            page.deinit();
            page = .{ .rows = &.{} };
            _ = page_arena.reset(.retain_capacity);
            page = try cursor.next(cursor.ptr, page_alloc, 2);
            point_pages += 1;
        }
        if (page.rows.len > 1) return error.InvalidSqlBackendResponse;
        if (page.rows.len == 0) {
            if (conflict_owner != null) return error.SqlWriteConflict;
            continue;
        }
        const previous = page.rows[0];
        if (!std.mem.eql(u8, previous.id, lookup_key)) return error.InvalidSqlBackendResponse;
        // excluded._id retains the proposed identity, but the actual update
        // addresses the native unique claim's physical owner.
        const proposed_key = mutation.key;
        mutation.key = try context.arena.dupe(u8, previous.id);
        mutation.expected_version = previous.version;
        mutation.unique_absence = false;
        mutation.expected_content_digest = previous.expected_content_digest;
        if (clause.assignments.len == 0) {
            mutation.predicate_only = true;
            mutation.row = null;
            mutation.json_null_fields = &.{};
            continue;
        }
        const width = binding.row_width;
        const cells = try page_alloc.alloc(scalar.Datum, binding.columns.len);
        for (0..width) |i| {
            const cell = try previous.cell(binding.columns[i].name);
            const column = if (i < table.columns.len) table.columns[i] else try table.column("_id");
            cells[i] = try @import("document_row.zig").declaredCell(page_alloc, column, cell);
            cells[width + i] = cells[i];
            const value = if (std.mem.eql(u8, binding.columns[i].name, "_id")) std.json.Value{ .string = proposed_key } else mutation.row.?.object.get(binding.columns[i].name) orelse .null;
            var sql_null = value == .null;
            for (mutation.json_null_fields) |field| if (std.mem.eql(u8, field, binding.columns[i].name)) {
                sql_null = false;
                break;
            };
            cells[width * 2 + i] = try @import("document_row.zig").declaredCell(page_alloc, column, .{ .value = value, .sql_null = sql_null });
        }
        for (captured_row, cells[width * 3 ..]) |capture, *cell| cell.* = capture;
        if (has_decisions) {
            // Point cursors have independent lifetimes. Retain at most one
            // bounded decision page, then resolve predicates and assignments
            // together before releasing it. Mutation fences remain unchanged.
            const scratch = decision_arena.allocator();
            const retained = try retainDecisionRow(scratch, mutation, previous, cells, table.columns);
            try pending.append(context.arena, retained);
            if (try page_budget.add(retained.cells)) {
                try applyDecisionConflicts(context, scratch, table, clause, binding, pending.items, deferred_cache);
                pending.clearRetainingCapacity();
                page_budget.rows = 0;
                page_budget.bytes = 0;
                if (!decision_arena.reset(.retain_capacity)) return error.OutOfMemory;
            }
            continue;
        }
        const matches = if (binding.predicate) |program| blk: {
            const value = try @import("decision_eval.zig").evaluateWithLimits(page_alloc, context.backend.decision_provider, &program, cells, context.parameters, @import("decision_eval.zig").limitsFor(context.backend));
            break :blk !value.sql_null and value.value == .bool and value.value.bool;
        } else true;
        if (!matches) {
            mutation.predicate_only = true;
            mutation.row = null;
            mutation.json_null_fields = &.{};
            continue;
        }
        var row: std.json.ObjectMap = .empty;
        var nulls: std.ArrayList([]const u8) = .empty;
        for (table.columns, 0..) |column, column_index| {
            if (column.generated) continue;
            var datum = cells[column_index];
            var assigned = false;
            if (binding.column_assignments[column_index]) |assignment_index| {
                const assignment = clause.assignments[assignment_index];
                // Omission, not NULL: native preparation owns DEFAULT and
                // generated values, only after this owner passed WHERE.
                if (assignment.use_default) continue;
                const program = binding.assignments[assignment_index];
                datum = if (assignment.capture_ordinal != null and assignment.capture_expression == null) blk: {
                    const ordinal = assignment.capture_ordinal.?;
                    if (ordinal >= captured_row.len) return error.InvalidSqlBackendResponse;
                    break :blk captured_row[ordinal];
                } else if (assignment.deferred_scalar) blk: {
                    const deferred = binding.deferred[assignment_index] orelse return error.InvalidSqlBackendResponse;
                    if (deferred_cache[assignment_index] == null) deferred_cache[assignment_index] = try context.deferredScalar(deferred.query, deferred.binding);
                    break :blk deferred_cache[assignment_index].?;
                } else try @import("decision_eval.zig").evaluateWithLimits(page_alloc, context.backend.decision_provider, &(program orelse return error.InvalidSqlBackendResponse), cells, context.parameters, @import("decision_eval.zig").limitsFor(context.backend));
                assigned = true;
            }
            if (!assigned and !try previous.hasField(column.name)) continue;
            if (column.type == .json and datum.value == .null and !datum.sql_null) try nulls.append(context.arena, column.name);
            try row.put(context.arena, column.name, try context.storageDatum(datum, column));
        }
        mutation.row = .{ .object = row };
        mutation.json_null_fields = nulls.items;
    }
    if (pending.items.len != 0) try applyDecisionConflicts(context, decision_arena.allocator(), table, clause, binding, pending.items, deferred_cache);
    return result;
}

const DecisionConflictRow = struct { mutation: *catalog.Mutation, presence: []const bool, cells: []const scalar.Datum };

/// Point pages retire before the external decision batch runs. Copy each old
/// payload once, sharing qualified aliases; only presence metadata is needed
/// from the original row. Do not retain a second complete preimage or directory.
fn retainDecisionRow(alloc: std.mem.Allocator, mutation: *catalog.Mutation, previous: catalog.Row, cells: []const scalar.Datum, columns: []const catalog.Column) !DecisionConflictRow {
    const width = columns.len + 1;
    if (cells.len < width * 3) return error.InvalidSqlBackendResponse;
    const presence = try alloc.alloc(bool, columns.len);
    const owned = try alloc.alloc(scalar.Datum, cells.len);
    // Native point scans may omit replaced columns. Presence is aligned with
    // the statement's declaration order below, not the sparse page directory.
    for (columns, presence) |column, *present| present.* = try previous.hasField(column.name);
    for (cells[0..width], owned[0..width], owned[width .. width * 2]) |cell, *out, *alias| {
        out.* = try @import("operators.zig").cloneDatum(alloc, cell);
        alias.* = out.*;
    }
    for (cells[width * 2 ..], owned[width * 2 ..]) |cell, *out| out.* = try @import("operators.zig").cloneDatum(alloc, cell);
    return .{ .mutation = mutation, .presence = presence, .cells = owned };
}

fn applyDecisionConflicts(context: anytype, scratch: std.mem.Allocator, table: catalog.Table, clause: ast.Conflict, binding: Bound, pending: []const DecisionConflictRow, deferred_cache: []?scalar.Datum) !void {
    try context.checkpoint();
    const all_cells = try scratch.alloc([]const scalar.Datum, pending.len);
    for (pending, all_cells) |row, *out| out.* = row.cells;
    const predicates = if (binding.predicate) |*program|
        try decision_eval.evaluateBatchWithLimits(scratch, context.backend.decision_provider, program, all_cells, context.parameters, @import("decision_eval.zig").limitsFor(context.backend))
    else
        null;
    var selected: std.ArrayList(DecisionConflictRow) = .empty;
    var cells: std.ArrayList([]const scalar.Datum) = .empty;
    for (pending, 0..) |row, index| {
        if (predicates) |values| {
            const value = values[index];
            if (!value.sql_null and value.value != .bool) return error.InvalidSqlBackendResponse;
            if (value.sql_null or !value.value.bool) {
                row.mutation.predicate_only = true;
                row.mutation.row = null;
                row.mutation.json_null_fields = &.{};
                continue;
            }
        }
        try selected.append(scratch, row);
        try cells.append(scratch, row.cells);
    }
    if (selected.items.len == 0) return;
    const assignment_values = try scratch.alloc([]const scalar.Datum, clause.assignments.len);
    for (clause.assignments, binding.assignments, assignment_values, 0..) |assignment, optional, *output, index| {
        if (assignment.use_default) {
            output.* = &.{};
        } else if (assignment.capture_ordinal != null and assignment.capture_expression == null) {
            const ordinal = binding.row_width * 3 + assignment.capture_ordinal.?;
            const values = try scratch.alloc(scalar.Datum, cells.items.len);
            for (cells.items, values) |row, *value| {
                if (ordinal >= row.len) return error.InvalidSqlBackendResponse;
                value.* = row[ordinal];
            }
            output.* = values;
        } else if (assignment.deferred_scalar) {
            const deferred = binding.deferred[index] orelse return error.InvalidSqlBackendResponse;
            if (deferred_cache[index] == null) deferred_cache[index] = try context.deferredScalar(deferred.query, deferred.binding);
            const values = try scratch.alloc(scalar.Datum, cells.items.len);
            @memset(values, deferred_cache[index].?);
            output.* = values;
        } else {
            const program = optional orelse return error.InvalidSqlBackendResponse;
            output.* = try decision_eval.evaluateBatchWithLimits(scratch, context.backend.decision_provider, &program, cells.items, context.parameters, @import("decision_eval.zig").limitsFor(context.backend));
        }
    }
    for (selected.items, 0..) |candidate, row_index| {
        try context.checkpoint();
        var row: std.json.ObjectMap = .empty;
        var nulls: std.ArrayList([]const u8) = .empty;
        for (table.columns, 0..) |column, column_index| {
            if (column.generated) continue;
            var datum = candidate.cells[column_index];
            var assigned = false;
            if (binding.column_assignments[column_index]) |index| {
                if (clause.assignments[index].use_default) continue;
                datum = assignment_values[index][row_index];
                assigned = true;
            }
            if (!assigned and !candidate.presence[column_index]) continue;
            if (column.type == .json and datum.value == .null and !datum.sql_null) try nulls.append(context.arena, column.name);
            try row.put(context.arena, column.name, try context.storageDatum(datum, column));
        }
        candidate.mutation.row = .{ .object = row };
        candidate.mutation.json_null_fields = nulls.items;
    }
}

fn conflictArrayOwnershipScenario(backing: std.mem.Allocator) !void {
    const Fixture = struct {
        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
            return error.UnexpectedBackendCall;
        }
        fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            return error.UnexpectedBackendCall;
        }
        fn mutate(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
            return error.UnexpectedBackendCall;
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const alloc = arena.allocator();
    const table: catalog.Table = .{ .id = 1, .physical_name = "items", .schema_version = 1, .columns = &.{
        .{ .name = "a", .path = "a", .type = .array, .element_type = .int64 },
        .{ .name = "j", .path = "j", .type = .array, .element_type = .jsonb },
        .{ .name = "n", .path = "n", .type = .integer },
        .{ .name = "missing", .path = "missing", .type = .string },
    } };
    var input = std.heap.ArenaAllocator.init(backing);
    var input_live = true;
    defer if (input_live) input.deinit();
    const a = input.allocator();
    const integers = try @import("array_text.zig").decodeLeaky(a, .int64, "[-1:1]={9007199254740993,NULL,2}", .{});
    const json = try @import("array_text.zig").decodeLeaky(a, .jsonb, "{\"null\",NULL}", .{});
    const layout = try catalog.Row.TypedLayout.init(a, &.{ "a", "j", "n", "missing" });
    const values = try a.dupe(scalar.Datum, &.{ scalar.Datum.typedArray(&integers.value), scalar.Datum.typedArray(&json.value), scalar.Datum.fromJson(.{ .integer = 1 }), .{} });
    const previous: catalog.Row = .{ .id = "identity", .version = 7, .value = .null, .typed_cells = .{ .layout = layout, .values = values, .presence = &.{ true, true, true, false } } };
    const width = table.columns.len + 1;
    const cells = try a.alloc(scalar.Datum, width * 3);
    @memcpy(cells[0..values.len], values);
    cells[values.len] = try previous.cell("_id");
    @memcpy(cells[width .. width * 2], cells[0..width]);
    @memcpy(cells[width * 2 ..], cells[0..width]);
    var mutation: catalog.Mutation = .{ .key = "identity", .expected_version = 7, .row = null };
    const retained = try retainDecisionRow(alloc, &mutation, previous, cells, table.columns);
    try std.testing.expect(retained.cells[0].array == retained.cells[width].array);
    try std.testing.expect(retained.cells[0].array != cells[0].array);
    input.deinit();
    input_live = false;
    const expression: ast.Scalar = .{ .literal = .{ .integer = 7 } };
    var program = try scalar.bindExpected(backing, &expression, &.{}, &.{}, .integer, .{});
    defer program.deinit();
    const clause: ast.Conflict = .{ .columns = &.{"_id"}, .assignments = &.{.{ .field = "n", .expression = &expression }} };
    const binding: Bound = .{ .columns = &.{}, .row_width = width, .assignments = &.{program}, .column_assignments = &.{ null, null, 0, null }, .predicate = null };
    var token: u8 = 0;
    const context: @import("runtime.zig").Context = .{
        .alloc = backing,
        .arena = alloc,
        .backend = .{ .ptr = &token, .vtable = &.{ .resolve = Fixture.resolve, .scan = Fixture.scan, .mutate = Fixture.mutate, .checkpoint = Fixture.checkpoint } },
        .binding = .{ .table = table, .action = .write, .columns = &.{}, .parameter_types = &.{}, .json_literals = .empty },
        .parameters = &.{},
        .limits = .{},
    };
    var cache = [_]?scalar.Datum{null};
    try applyDecisionConflicts(context, alloc, table, clause, binding, &.{retained}, &cache);
    try std.testing.expectEqual(@as(u64, 7), mutation.expected_version);
    try std.testing.expectEqual(@as(i64, 7), mutation.row.?.object.get("n").?.integer);
    try std.testing.expect(!mutation.row.?.object.contains("missing"));
    try std.testing.expectEqual(@as(usize, 0), mutation.json_null_fields.len);
    const array = try @import("array_wire.zig").decodeBorrowed(alloc, .int64, mutation.row.?.object.get("a").?, .{});
    try std.testing.expectEqual(@as(i32, -1), array.value.dimensions[0].lower);
    try std.testing.expectEqual(@as(i64, 9007199254740993), array.value.elements[0].value.integer);
    try std.testing.expect(array.value.elements[1].sql_null);
    const json_array = try @import("array_wire.zig").decodeBorrowed(alloc, .jsonb, mutation.row.?.object.get("j").?, .{});
    try std.testing.expect(json_array.value.elements[0].value == .null);
    try std.testing.expect(!json_array.value.elements[0].sql_null);
    try std.testing.expect(json_array.value.elements[1].sql_null);
}

test "SQL conflict decision batches own arrays once across page retirement and unwind every allocation failure" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, conflictArrayOwnershipScenario, .{});
}

/// Targetless DO NOTHING arbitrates every native unique generation and the
/// physical row key. Only accepted rows reserve statement-local identities;
/// rejected candidates must not shadow later candidates in the VALUES list.
fn resolveAny(context: anytype, table: catalog.Table, binding: Bound, proposed: []const catalog.Mutation, owners: []const catalog.ConflictOwner) ![]const catalog.Mutation {
    var accepted_keys: std.StringHashMapUnmanaged(void) = .empty;
    var accepted_claims: std.StringHashMapUnmanaged(void) = .empty;
    var result: std.ArrayList(catalog.Mutation) = .empty;
    for (proposed, owners) |candidate, owner| {
        try context.checkpoint();
        if (owner.primary_only) {
            if (owner.guard != null or owner.key != null or owner.identity != null or owner.identities.len != 0) return error.InvalidSqlBackendResponse;
        } else if (owner.guard == null or (owner.key != null and owner.identity == null)) return error.InvalidSqlBackendResponse;
        if (accepted_keys.contains(candidate.key)) continue;
        const duplicate = for (owner.identities) |identity| {
            if (accepted_claims.contains(identity)) break true;
        } else false;
        if (duplicate) continue;
        var point = candidate;
        // A native unique owner alone is sufficient to skip the candidate.
        // Retain its point fence as well as all native claim comparisons.
        if (owner.key) |key| point.key = key;
        const resolved = try resolvePrepared(context, table, .{ .columns = &.{"_id"} }, binding, &.{point}, &.{point}, null, &.{});
        if (resolved.len != 1) return error.InvalidSqlBackendResponse;
        var mutation = resolved[0];
        if (owner.key != null and !mutation.predicate_only) return error.SqlWriteConflict;
        mutation.conflict_guard = owner.guard;
        try result.append(context.arena, mutation);
        if (!mutation.predicate_only) {
            try accepted_keys.put(context.arena, candidate.key, {});
            for (owner.identities) |identity| try accepted_claims.put(context.arena, identity, {});
        }
    }
    return result.items;
}
