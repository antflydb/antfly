// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Native-fenced primary and unique arbiters with compiled old/excluded expressions.
//! Every observed row (including a skipped row) remains a native commit fence.
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const scalar = @import("scalar.zig");

pub const Bound = struct {
    columns: []const scalar.Column,
    assignments: []const scalar.Program,
    predicate: ?scalar.Program,
};

pub fn bind(alloc: std.mem.Allocator, backend: catalog.Backend, table: catalog.Table, name: ast.Name, clause: ast.Conflict, parameters: []?ast.ColumnType) !Bound {
    // Secondary unique arbiters require a native unique-key reservation, not
    // a scan of a possibly partial index. Refuse them until that authority is
    // exposed by the native coordinator.
    if (!primary(clause)) {
        if (backend.vtable.resolve_conflict_owners == null) return error.UnsupportedSqlShape;
        for (clause.columns) |column| _ = try table.column(column);
    }
    const count = table.columns.len + 1;
    const columns = try alloc.alloc(scalar.Column, count * 3);
    for (0..count) |i| {
        const column = if (i == table.columns.len) try table.column("_id") else table.columns[i];
        columns[i] = .{ .name = column.name, .type = column.type, .nullable = column.nullable };
        columns[count + i] = .{ .name = try std.fmt.allocPrint(alloc, "{s}\x00{s}", .{ name.table, column.name }), .type = column.type, .nullable = column.nullable };
        columns[count * 2 + i] = .{ .name = try std.fmt.allocPrint(alloc, "excluded\x00{s}", .{column.name}), .type = column.type, .nullable = column.nullable };
    }
    var pass: usize = 0;
    while (true) : (pass += 1) {
        if (pass > parameters.len + 1) return error.ConflictingSqlParameterTypes;
        var changed = false;
        for (clause.assignments) |assignment| {
            const column = try table.column(assignment.field);
            if (column.generated or std.mem.eql(u8, column.name, "_id")) return error.UnsupportedSqlShape;
            const expression = assignment.expression orelse return error.InvalidSqlBackendResponse;
            changed = try scalar.inferParameters(alloc, expression, columns, parameters, column.type, .{}) or changed;
        }
        if (clause.predicate) |expression| changed = try scalar.inferParameters(alloc, expression, columns, parameters, .boolean, .{}) or changed;
        if (!changed) break;
    }
    const assignments = try alloc.alloc(scalar.Program, clause.assignments.len);
    for (clause.assignments, assignments) |assignment, *program| program.* = try scalar.bindExpected(alloc, assignment.expression.?, columns, parameters, (try table.column(assignment.field)).type, .{});
    return .{ .columns = columns, .assignments = assignments, .predicate = if (clause.predicate) |expression| try scalar.bindExpected(alloc, expression, columns, parameters, .boolean, .{}) else null };
}

pub fn primary(clause: ast.Conflict) bool {
    return clause.columns.len == 1 and std.mem.eql(u8, clause.columns[0], "_id");
}

pub fn allowsDuplicateKeys(clause: ast.Conflict) bool {
    return clause.assignments.len == 0 and (primary(clause) or clause.columns.len == 0);
}

/// A retained point snapshot plus an atomic version predicate is optimistic
/// concurrency control, not a read-then-overwrite. A racing insert/update is a
/// definite serialization conflict, never an automatically replayed mutation.
pub fn resolve(context: anytype, table: catalog.Table, clause: ast.Conflict, binding: Bound, proposed: []const catalog.Mutation) ![]const catalog.Mutation {
    if (!context.backend.predicate_only_mutations) return error.UnsupportedSqlExecution;
    if (table.storage_mode != .relational) return error.UnsupportedSqlExecution;
    const prepare = context.backend.vtable.prepare_mutations orelse return error.UnsupportedSqlExecution;
    const normalized = try prepare(context.backend.ptr, context.arena, table, proposed);
    if (normalized.len != proposed.len) return error.InvalidSqlBackendResponse;
    const owners = if (!primary(clause)) try (context.backend.vtable.resolve_conflict_owners orelse return error.UnsupportedSqlExecution)(context.backend.ptr, context.arena, table, clause.columns, normalized) else null;
    if (owners) |items| if (items.len != normalized.len) return error.InvalidSqlBackendResponse;
    if (clause.columns.len == 0) {
        for (proposed, normalized) |original, value| {
            if (!std.mem.eql(u8, original.key, value.key) or value.row == null or value.expected_version != 0) return error.InvalidSqlBackendResponse;
        }
        return resolveAny(context, table, binding, normalized, owners.?);
    }
    return resolvePrepared(context, table, clause, binding, proposed, normalized, owners);
}

fn resolvePrepared(context: anytype, table: catalog.Table, clause: ast.Conflict, binding: Bound, proposed: []const catalog.Mutation, normalized: []const catalog.Mutation, owners: ?[]const catalog.ConflictOwner) ![]const catalog.Mutation {
    const buffer = try context.arena.alloc(catalog.Mutation, normalized.len);
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
        count += 1;
    }
    const result = buffer[0..count];
    var fields: std.ArrayList([]const u8) = .empty;
    if (clause.assignments.len != 0) for (table.columns, 0..) |column, ordinal| {
        // Preserve ordinary columns for replacement; generated columns are
        // recomputed natively and fetched only if old-row expressions need one.
        const replaced = for (clause.assignments) |assignment| {
            if (std.mem.eql(u8, assignment.field, column.name)) break true;
        } else false;
        var needed = !column.generated and !replaced;
        const width = binding.columns.len / 3;
        for (binding.assignments) |program| for (program.required_columns) |required| {
            if (required < width * 2 and required % width == ordinal) needed = true;
        };
        if (binding.predicate) |program| for (program.required_columns) |required| {
            if (required < width * 2 and required % width == ordinal) needed = true;
        };
        if (needed) try fields.append(context.arena, column.path);
    };
    for (result) |*mutation| {
        try context.checkpoint();
        const conflict_owner = owner_by_key.get(mutation.key);
        if (conflict_owner) |owner| if (owner.key == null) continue;
        const lookup_key = if (conflict_owner) |owner| owner.key.? else mutation.key;
        const open = context.backend.vtable.open_scan orelse return error.SqlStatementSnapshotRequired;
        const cursor = (try open(context.backend.ptr, context.arena, table, .{ .fields = fields.items, .primary_key = lookup_key, .limit = 1, .include_primary_digest = true })) orelse return error.SqlStatementSnapshotRequired;
        defer cursor.close(cursor.ptr);
        var page = try cursor.next(cursor.ptr, context.arena, 2);
        defer page.deinit();
        var pages: usize = 1;
        // Native point ranges may yield an empty progress page while skipping
        // expired rows; only exhaustion proves absence. A row-filled page can
        // carry continuation even though the exact key already resolves.
        while (page.rows.len == 0 and page.after != null) {
            try context.checkpoint();
            if (pages >= context.limits.scan_pages) return error.SqlProgramLimitExceeded;
            page.deinit();
            page = .{ .rows = &.{} };
            page = try cursor.next(cursor.ptr, context.arena, 2);
            pages += 1;
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
        mutation.expected_content_digest = previous.expected_content_digest;
        if (clause.assignments.len == 0) {
            mutation.predicate_only = true;
            mutation.row = null;
            mutation.json_null_fields = &.{};
            continue;
        }
        const width = binding.columns.len / 3;
        const cells = try context.arena.alloc(scalar.Datum, binding.columns.len);
        for (0..width) |i| {
            const cell = try previous.cell(binding.columns[i].name);
            cells[i] = .{ .value = try @import("describe.zig").coerce(cell.value, binding.columns[i].type), .sql_null = cell.sql_null };
            cells[width + i] = cells[i];
            const value = if (std.mem.eql(u8, binding.columns[i].name, "_id")) std.json.Value{ .string = proposed_key } else mutation.row.?.object.get(binding.columns[i].name) orelse .null;
            var sql_null = value == .null;
            for (mutation.json_null_fields) |field| if (std.mem.eql(u8, field, binding.columns[i].name)) {
                sql_null = false;
                break;
            };
            cells[width * 2 + i] = .{ .value = try @import("describe.zig").coerce(value, binding.columns[i].type), .sql_null = sql_null };
        }
        const matches = if (binding.predicate) |program| blk: {
            const value = try program.evaluate(context.arena, cells, context.parameters, .{});
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
        for (table.columns) |column| {
            if (column.generated) continue;
            var datum: scalar.Datum = blk: {
                const old = try previous.cell(column.name);
                break :blk .{ .value = old.value, .sql_null = old.sql_null };
            };
            for (clause.assignments, binding.assignments) |assignment, program| if (std.mem.eql(u8, assignment.field, column.name)) {
                datum = try program.evaluate(context.arena, cells, context.parameters, .{});
                break;
            };
            if (datum.sql_null and !column.nullable) return error.SqlNotNullViolation;
            if (datum.value == .null and !datum.sql_null) try nulls.append(context.arena, column.name);
            try row.put(context.arena, column.name, try @import("runtime.zig").clone(context.arena, try @import("describe.zig").coerce(datum.value, column.type)));
        }
        mutation.row = .{ .object = row };
        mutation.json_null_fields = nulls.items;
    }
    return result;
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
        const resolved = try resolvePrepared(context, table, .{ .columns = &.{"_id"} }, binding, &.{point}, &.{point}, null);
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
