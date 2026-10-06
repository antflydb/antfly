// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Joined DML reuses the typed relation engine and a single statement capture.
//! Target provenance travels with that captured row, never through a later
//! point lookup. All images and RETURNING values are prepared before commit.
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const compiler = @import("compiler.zig");
const describe = @import("describe.zig");
const runtime = @import("runtime.zig");
const Allocator = std.mem.Allocator;

pub const metadata_fields = [_][]const u8{ "\x00mutation_version", "\x00mutation_digest", "\x00mutation_document", "\x00mutation_presence" };
pub fn isMetadata(name: []const u8) bool {
    for (metadata_fields) |field| if (std.mem.eql(u8, name, field)) return true;
    return false;
}
pub fn cell(alloc: Allocator, row: catalog.Row, name: []const u8) !catalog.Row.Cell {
    if (std.mem.eql(u8, name, metadata_fields[0])) return .{ .value = .{ .string = try std.fmt.allocPrint(alloc, "{d}", .{row.version}) }, .sql_null = false };
    if (std.mem.eql(u8, name, metadata_fields[1])) return .{ .value = .{ .string = if (row.expected_content_digest) |digest| try alloc.dupe(u8, &std.fmt.bytesToHex(digest, .lower)) else "" }, .sql_null = false };
    if (std.mem.eql(u8, name, metadata_fields[2])) return .{ .value = row.document orelse .null, .sql_null = row.document == null };
    if (std.mem.eql(u8, name, metadata_fields[3])) {
        if (row.value != .object) return error.InvalidSqlBackendResponse;
        var size: usize = 0;
        for (row.value.object.keys()) |key| {
            if (key.len > std.math.maxInt(u16)) return error.SqlProgramLimitExceeded;
            size = std.math.add(usize, size, key.len + 2) catch return error.SqlProgramLimitExceeded;
        }
        const encoded = try alloc.alloc(u8, size);
        var cursor: usize = 0;
        for (row.value.object.keys()) |key| {
            encoded[cursor] = @truncate(key.len);
            encoded[cursor + 1] = @truncate(key.len >> 8);
            @memcpy(encoded[cursor + 2 ..][0..key.len], key);
            cursor += key.len + 2;
        }
        return .{ .value = .{ .string = encoded }, .sql_null = false };
    }
    return row.cell(name);
}

fn presenceContains(encoded: []const u8, name: []const u8) !bool {
    var cursor: usize = 0;
    while (cursor < encoded.len) {
        if (encoded.len - cursor < 2) return error.InvalidSqlBackendResponse;
        const len = @as(usize, encoded[cursor]) | (@as(usize, encoded[cursor + 1]) << 8);
        cursor += 2;
        if (len > encoded.len - cursor) return error.InvalidSqlBackendResponse;
        if (std.mem.eql(u8, encoded[cursor..][0..len], name)) return true;
        cursor += len;
    }
    return false;
}

test "joined mutation presence distinguishes omitted cells from present SQL null" {
    const alloc = std.testing.allocator;
    var object: std.json.ObjectMap = .empty;
    defer object.deinit(alloc);
    try object.put(alloc, "nullable", .null);
    try object.put(alloc, "nullable.extra", .{ .integer = 4 });
    const row: catalog.Row = .{ .id = "r", .version = 1, .value = .{ .object = object }, .sql_nulls = &.{ true, false } };
    const encoded = try cell(alloc, row, metadata_fields[3]);
    defer alloc.free(encoded.value.string);
    try std.testing.expect(!encoded.sql_null);
    try std.testing.expect(try presenceContains(encoded.value.string, "nullable"));
    try std.testing.expect(try presenceContains(encoded.value.string, "nullable.extra"));
    try std.testing.expect(!try presenceContains(encoded.value.string, "missing"));
    try std.testing.expectError(error.InvalidSqlBackendResponse, presenceContains(&.{ 4, 0, 'x' }, "x"));
}

pub const Bound = struct {
    input: *const describe.BoundStatement,
    query: ast.Select,
    fields: []const catalog.Column,
    preserve: []const bool,
    default_paths: []const []const u8,
    deleting: bool,
    returning: ?[]const ast.Projection,
};

fn column(alloc: Allocator, qualifier: []const u8, name: []const u8) !*const ast.Scalar {
    const out = try alloc.create(ast.Scalar);
    out.* = .{ .column = try std.fmt.allocPrint(alloc, "{s}\x00{s}", .{ qualifier, name }) };
    return out;
}

pub fn bind(alloc: Allocator, backend: catalog.Backend, table: catalog.Table, compiled: *const compiler.Compiled, parameters: []?ast.ColumnType) !Bound {
    const deleting = compiled.statement == .delete;
    var source = if (deleting) compiled.statement.delete.source.? else compiled.statement.update.source.?;
    const name = if (deleting) compiled.statement.delete.table else compiled.statement.update.table;
    const alias = (if (deleting) compiled.statement.delete.alias else compiled.statement.update.alias) orelse name.table;
    const assignments: []const ast.Assignment = if (deleting) &.{} else compiled.statement.update.assignments;
    const returning = if (deleting) compiled.statement.delete.returning else compiled.statement.update.returning;
    for (assignments, 0..) |assignment, i| {
        const field = try table.column(assignment.field);
        if (field.generated and !assignment.use_default) return error.SqlGeneratedColumnWrite;
        if (std.mem.eql(u8, field.name, "_id")) return error.UnsupportedSqlExecution;
        for (assignments[0..i]) |prior| if (std.mem.eql(u8, prior.field, field.name)) return error.DuplicateSqlColumn;
    }
    var projections: std.ArrayList(ast.Projection) = .empty;
    var expected: std.ArrayList(ast.ColumnType) = .empty;
    for ([_][]const u8{ "_id", metadata_fields[0], metadata_fields[1], metadata_fields[2], metadata_fields[3] }, 0..) |field, i| {
        try projections.append(alloc, .{ .expression = try column(alloc, alias, field) });
        try expected.append(alloc, if (i == 3) .json else .string);
    }
    var fields: std.ArrayList(catalog.Column) = .empty;
    var preserve: std.ArrayList(bool) = .empty;
    var default_paths: std.ArrayList([]const u8) = .empty;
    for (table.columns) |field| {
        if (deleting and returning == null) continue;
        if (!deleting and field.generated) continue;
        var expression: ?*const ast.Scalar = null;
        var use_default = false;
        for (assignments) |assignment| if (std.mem.eql(u8, assignment.field, field.name)) {
            use_default = assignment.use_default;
            if (!use_default) expression = assignment.expression orelse blk: {
                const literal = try alloc.create(ast.Scalar);
                literal.* = .{ .literal = assignment.value };
                break :blk literal;
            };
            break;
        };
        if (use_default) {
            try default_paths.append(alloc, field.path);
            continue;
        }
        if (!deleting and table.storage_mode == .document and expression == null) continue;
        try projections.append(alloc, .{ .expression = expression orelse try column(alloc, alias, field.name) });
        try expected.append(alloc, field.type);
        try fields.append(alloc, field);
        try preserve.append(alloc, expression == null);
    }
    const predicate = if (deleting) compiled.statement.delete.predicate else compiled.statement.update.predicate;
    // FROM/USING starts as a cross join. Expose safe equality conjuncts to
    // the relation planner so a keyed mutation is O(source + target), not
    // O(source * target). Keep the complete WHERE as the final residual.
    if (source.* == .join and source.join.kind == .cross) if (predicate) |filter| {
        if (filter.* == .scalar) if (try equalityConjuncts(alloc, filter.scalar)) |condition| {
            const keyed = try alloc.create(ast.Relation);
            keyed.* = .{ .join = .{ .kind = .inner, .left = source.join.left, .right = source.join.right, .condition = condition } };
            source = keyed;
        };
    };
    const groups: []*const ast.Scalar = if (deleting) try alloc.alloc(*const ast.Scalar, projections.items.len) else &.{};
    // DELETE is a target-set operation. Deduplicate in the bounded streaming
    // group operator before applying the mutation-row quota: source fanout
    // must not consume one retained mutation image per duplicate match.
    if (deleting) for (projections.items, groups) |projection, *group| {
        group.* = projection.expression.?;
    };
    const query: ast.Select = .{ .source = source, .ctes = if (deleting) compiled.statement.delete.ctes else compiled.statement.update.ctes, .columns = projections.items, .predicate = predicate, .group_by = groups };
    // The target was already resolved/authorized for read+write. Reuse that
    // immutable binding; resolve every other physical source for read access.
    var adapter: @import("relation_binding.zig").TargetResolveAdapter = .{ .backend = backend, .table = table, .name = name };
    try @import("relation_binding.zig").inferExpected(alloc, adapter.iface(), query, parameters, expected.items);
    const selected: compiler.Compiled = .{ .arena = undefined, .statement = .{ .select = query }, .parameter_count = compiled.parameter_count };
    const input = try alloc.create(describe.BoundStatement);
    input.* = try describe.bind(alloc, adapter.iface(), &selected, parameters);
    for (input.columns, expected.items) |actual, required| {
        if (!actual.untyped_null and actual.type != required and !(actual.type == .integer and required == .number)) return error.SqlTypeMismatch;
    }
    return .{ .input = input, .query = query, .fields = fields.items, .preserve = preserve.items, .default_paths = default_paths.items, .deleting = deleting, .returning = returning };
}

fn equalityConjuncts(alloc: Allocator, expression: *const ast.Scalar) anyerror!?*const ast.Scalar {
    if (expression.* != .binary) return null;
    const binary = expression.binary;
    if (binary.op == .eq and scalarOnly(binary.left) and scalarOnly(binary.right)) return expression;
    if (binary.op != .@"and") return null;
    const left = try equalityConjuncts(alloc, binary.left);
    const right = try equalityConjuncts(alloc, binary.right);
    if (left == null) return right;
    if (right == null) return left;
    const combined = try alloc.create(ast.Scalar);
    combined.* = .{ .binary = .{ .op = .@"and", .left = left.?, .right = right.? } };
    return combined;
}

fn scalarOnly(expression: *const ast.Scalar) bool {
    return switch (expression.*) {
        .literal, .column => true,
        .unary => |v| scalarOnly(v.operand),
        .cast => |v| scalarOnly(v.operand),
        .binary => |v| scalarOnly(v.left) and scalarOnly(v.right),
        .call => |v| blk: {
            if (v.subquery != null or v.window != null or v.filter != null or v.star or v.distinct) break :blk false;
            for (v.args) |arg| if (!scalarOnly(arg)) break :blk false;
            break :blk true;
        },
        .case_when => |v| blk: {
            for (v.branches) |branch| if (!scalarOnly(branch.condition) or !scalarOnly(branch.value)) break :blk false;
            break :blk if (v.otherwise) |other| scalarOnly(other) else true;
        },
        .in_list => |v| blk: {
            if (!scalarOnly(v.operand)) break :blk false;
            for (v.values) |value| if (!scalarOnly(value)) break :blk false;
            break :blk true;
        },
    };
}

pub fn execute(context: anytype, bound: Bound) !runtime.Output {
    var read = context;
    read.binding = bound.input.*;
    read.typed_output = true;
    read.limits.result_rows = context.limits.mutation_rows;
    const selected = try read.select(bound.query);
    const flags = selected.sql_nulls orelse if (selected.rows.len == 0) &.{} else return error.InvalidSqlBackendResponse;
    if (flags.len != selected.rows.len or selected.rows.len > context.limits.mutation_rows) return error.InvalidSqlBackendResponse;
    var mutations: std.ArrayList(catalog.Mutation) = .empty;
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    const table = context.binding.table.?;
    for (selected.rows, flags) |values, nulls| {
        try context.checkpoint();
        if (values.len != bound.fields.len + 5 or nulls.len != values.len) return error.InvalidSqlBackendResponse;
        if (nulls[0]) continue; // An outer join may have no target row.
        if (values[0] != .string or nulls[1] or values[1] != .string or nulls[2] or values[2] != .string) return error.InvalidSqlBackendResponse;
        const key = values[0].string;
        if ((try seen.getOrPut(context.arena, key)).found_existing) {
            if (bound.deleting) continue;
            return error.SqlMutationCardinalityViolation;
        }
        var digest: ?[32]u8 = null;
        if (values[2].string.len != 0) {
            var bytes: [32]u8 = undefined;
            if (values[2].string.len != 64) return error.InvalidSqlBackendResponse;
            _ = std.fmt.hexToBytes(&bytes, values[2].string) catch return error.InvalidSqlBackendResponse;
            digest = bytes;
        }
        const version = std.fmt.parseInt(u64, values[1].string, 10) catch return error.InvalidSqlBackendResponse;
        if (table.storage_mode == .document and version != 0 and digest == null) return error.InvalidSqlBackendResponse;
        if (nulls[4] or values[4] != .string) return error.InvalidSqlBackendResponse;
        const present = values[4].string;
        var object: std.json.ObjectMap = .empty;
        var json_null_fields: std.ArrayList([]const u8) = .empty;
        var old_flags: std.ArrayList(bool) = .empty;
        if (!bound.deleting and table.storage_mode == .document) {
            if (nulls[3] or values[3] != .object) return error.InvalidSqlBackendResponse;
            var iter = values[3].object.iterator();
            while (iter.next()) |member| {
                const declared = table.column(member.key_ptr.*) catch null;
                if (declared) |field| if (field.generated) continue;
                const overwritten = for (bound.fields) |field| {
                    if (std.mem.eql(u8, field.path, member.key_ptr.*)) break true;
                } else false;
                if (overwritten) continue;
                const reset = for (bound.default_paths) |path| {
                    if (std.mem.eql(u8, path, member.key_ptr.*)) break true;
                } else false;
                if (reset) continue;
                try object.put(context.arena, member.key_ptr.*, member.value_ptr.*);
                if (declared) |field| if (field.type == .json and member.value_ptr.* == .null) try json_null_fields.append(context.arena, field.path);
            }
        }
        for (bound.fields, bound.preserve, values[5..], nulls[5..]) |field, preserve, value, is_null| {
            if (preserve and !try presenceContains(present, field.name)) continue;
            if (!bound.deleting and is_null and !field.nullable) return error.SqlNotNullViolation;
            try object.put(context.arena, field.path, if (is_null) .null else try describe.coerce(value, field.type));
            if (!is_null and value == .null) try json_null_fields.append(context.arena, field.path);
            try old_flags.append(context.arena, is_null);
        }
        const previous = if (bound.deleting and bound.returning != null) blk: {
            const old = try context.arena.create(catalog.Row);
            old.* = .{ .id = key, .version = version, .value = .{ .object = object }, .sql_nulls = old_flags.items };
            break :blk old;
        } else null;
        try mutations.append(context.arena, .{ .key = key, .expected_version = version, .expected_content_digest = digest, .row = if (bound.deleting) null else .{ .object = object }, .json_null_fields = json_null_fields.items, .previous = previous });
    }
    return context.commitMutations(table, mutations.items, if (bound.deleting) "DELETE" else "UPDATE", bound.returning);
}
