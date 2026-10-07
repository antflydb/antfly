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

//! Joined DML reuses the typed relation engine and a single statement capture.
//! Target provenance travels with that captured row, never through a later
//! point lookup. All images and RETURNING values are prepared before commit.
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const compiler = @import("compiler.zig");
const describe = @import("describe.zig");
const runtime = @import("runtime.zig");
const scalar = @import("scalar.zig");
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
        const names = try row.fieldNames();
        var size: usize = 0;
        for (names) |key| {
            if (!try row.hasField(key)) continue;
            if (key.len > std.math.maxInt(u16)) return error.SqlProgramLimitExceeded;
            size = std.math.add(usize, size, key.len + 2) catch return error.SqlProgramLimitExceeded;
        }
        const encoded = try alloc.alloc(u8, size);
        var cursor: usize = 0;
        for (names) |key| {
            if (!try row.hasField(key)) continue;
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

/// Build once per source row, borrowing names from its pinned metadata. Wide
/// replacement images must not rescan this variable-width directory per cell.
fn presenceDirectory(alloc: Allocator, encoded: []const u8) !std.StringHashMapUnmanaged(void) {
    var directory: std.StringHashMapUnmanaged(void) = .empty;
    errdefer directory.deinit(alloc);
    var offset: usize = 0;
    while (offset < encoded.len) {
        if (encoded.len - offset < 2) return error.InvalidSqlBackendResponse;
        const length = @as(usize, encoded[offset]) | (@as(usize, encoded[offset + 1]) << 8);
        offset += 2;
        if (length > encoded.len - offset) return error.InvalidSqlBackendResponse;
        if ((try directory.getOrPut(alloc, encoded[offset..][0..length])).found_existing) return error.InvalidSqlBackendResponse;
        offset += length;
    }
    return directory;
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
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const layout = try catalog.Row.TypedLayout.init(arena.allocator(), &.{ "nullable", "missing" });
    const typed: catalog.Row = .{ .id = "r", .version = 1, .value = .null, .typed_cells = .{ .layout = layout, .values = &.{ .{}, .{} }, .presence = &.{ true, false } } };
    const typed_encoded = try cell(arena.allocator(), typed, metadata_fields[3]);
    try std.testing.expect(try presenceContains(typed_encoded.value.string, "nullable"));
    try std.testing.expect(!try presenceContains(typed_encoded.value.string, "missing"));
}

test "joined mutation presence directories reject duplicate and truncated names" {
    const alloc = std.testing.allocator;
    var directory = try presenceDirectory(alloc, &.{ 1, 0, 'a', 2, 0, 'b', 'c' });
    defer directory.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), directory.count());
    try std.testing.expect(directory.contains("a") and directory.contains("bc"));
    try std.testing.expect(!directory.contains("b"));
    try std.testing.expectError(error.InvalidSqlBackendResponse, presenceDirectory(alloc, &.{ 1, 0, 'a', 1, 0, 'a' }));
    try std.testing.expectError(error.InvalidSqlBackendResponse, presenceDirectory(alloc, &.{ 2, 0, 'a' }));
    try std.testing.expectError(error.InvalidSqlBackendResponse, presenceDirectory(alloc, &.{1}));
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
    var expected: std.ArrayList(scalar.Type) = .empty;
    for ([_][]const u8{ "_id", metadata_fields[0], metadata_fields[1], metadata_fields[2], metadata_fields[3] }, 0..) |field, i| {
        try projections.append(alloc, .{ .expression = try column(alloc, alias, field) });
        try expected.append(alloc, .{ .kind = if (i == 3) .json else .string });
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
        const required: scalar.Type = .{ .kind = field.type, .element_type = field.element_type };
        try projections.append(alloc, .{ .expression = if (expression) |assigned| try scalar.assignmentExpression(alloc, assigned, required) else try column(alloc, alias, field.name) });
        try expected.append(alloc, required);
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
    try @import("relation_binding.zig").inferExpectedTypes(alloc, adapter.iface(), query, parameters, expected.items);
    const selected: compiler.Compiled = .{ .arena = undefined, .statement = .{ .select = query }, .parameter_count = compiled.parameter_count };
    const input = try alloc.create(describe.BoundStatement);
    input.* = try describe.bind(alloc, adapter.iface(), &selected, parameters);
    for (input.columns, expected.items) |actual, required| {
        if (!actual.untyped_null and actual.type != required.kind and !(actual.type == .integer and required.kind == .number)) return if (required.kind == .array) error.SqlAssignmentTypeMismatch else error.SqlTypeMismatch;
        if (actual.type == .array and required.kind == .array and !@import("builtin_cast.zig").assignmentAllowed(actual.element_type orelse return error.SqlAssignmentTypeMismatch, required.element_type orelse return error.SqlAssignmentTypeMismatch)) return error.SqlAssignmentTypeMismatch;
    }
    // FROM/USING columns also participate in RETURNING name resolution.
    // Validate against the existing authorized input scope before projecting
    // prepared target images; do not silently resolve an ambiguous bare name
    // to the target, or perform another catalog/read capture for this check.
    if (returning) |projections_| {
        const relation = input.relation orelse return error.InvalidSqlBackendResponse;
        for (projections_) |projection| {
            if (projection.wildcard) continue;
            const field: ast.Scalar = .{ .column = projection.field };
            _ = try @import("relation_binding.zig").lowerBoundExpression(alloc, relation.root.columns, projection.expression orelse &field);
        }
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
    var mutations: std.ArrayList(catalog.Mutation) = .empty;
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    const table = context.binding.table.?;
    var old_layout: ?catalog.Row.TypedLayout = null;
    if (bound.deleting and bound.returning != null) {
        const names = try context.arena.alloc([]const u8, bound.fields.len);
        for (bound.fields, names) |field, *name| name.* = field.path;
        old_layout = try catalog.Row.TypedLayout.init(context.arena, names);
    }
    // Materialize the captured source once, retaining complete typed values in
    // the shared bounded/spillable cursor. Drain and close it before writer
    // admission; per-row scratch never becomes a mutation's payload owner.
    {
        const selected = try read.typedQuery(bound.query);
        defer selected.close();
        if (selected.count() > context.limits.mutation_rows) return error.SqlProgramLimitExceeded;
        // One borrowed forward pass releases finished resident sort rows and
        // never writes a replay run for an external sort. Only retained image
        // fields cross the owned storage/preimage boundary below.
        var scratch = std.heap.ArenaAllocator.init(context.alloc);
        defer scratch.deinit();
        const temporary = scratch.allocator();
        while (true) {
            _ = scratch.reset(.retain_capacity);
            const values = (try selected.nextBorrowed(temporary)) orelse break;
            try context.checkpoint();
            if (values.len != bound.fields.len + 5) return error.InvalidSqlBackendResponse;
            if (values[0].sql_null) continue; // An outer join may have no target row.
            if (values[0].value != .string or values[1].sql_null or values[1].value != .string or values[2].sql_null or values[2].value != .string) return error.InvalidSqlBackendResponse;
            if (seen.contains(values[0].value.string)) {
                if (bound.deleting) continue;
                return error.SqlMutationCardinalityViolation;
            }
            const key = try context.arena.dupe(u8, values[0].value.string);
            try seen.put(context.arena, key, {});
            var digest: ?[32]u8 = null;
            if (values[2].value.string.len != 0) {
                var bytes: [32]u8 = undefined;
                if (values[2].value.string.len != 64) return error.InvalidSqlBackendResponse;
                _ = std.fmt.hexToBytes(&bytes, values[2].value.string) catch return error.InvalidSqlBackendResponse;
                digest = bytes;
            }
            const version = std.fmt.parseInt(u64, values[1].value.string, 10) catch return error.InvalidSqlBackendResponse;
            if (table.storage_mode == .document and version != 0 and digest == null) return error.InvalidSqlBackendResponse;
            if (values[4].sql_null or values[4].value != .string) return error.InvalidSqlBackendResponse;
            const present = try presenceDirectory(temporary, values[4].value.string);
            var object: std.json.ObjectMap = .empty;
            var json_null_fields: std.ArrayList([]const u8) = .empty;
            if (!bound.deleting and table.storage_mode == .document) {
                if (values[3].sql_null or values[3].value != .object) return error.InvalidSqlBackendResponse;
                var iter = values[3].value.object.iterator();
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
                    try object.put(context.arena, try context.arena.dupe(u8, member.key_ptr.*), try runtime.clone(context.arena, member.value_ptr.*));
                    if (declared) |field| if (field.type == .json and member.value_ptr.* == .null) try json_null_fields.append(context.arena, field.path);
                }
            }
            const previous = if (bound.deleting and bound.returning != null) blk: {
                const old = try context.arena.create(catalog.Row);
                old.* = try catalog.Row.fromDatums(context.arena, key, old_layout.?, values[5..]);
                old.version = version;
                old.expected_content_digest = digest;
                if (!values[3].sql_null) old.document = try runtime.clone(context.arena, values[3].value);
                const presence = try context.arena.alloc(bool, bound.fields.len);
                for (bound.fields, presence) |field, *flag| flag.* = present.contains(field.name);
                old.typed_cells.?.presence = presence;
                break :blk old;
            } else null;
            if (!bound.deleting) for (bound.fields, bound.preserve, values[5..]) |field, preserve, datum| {
                if (preserve and !present.contains(field.name)) continue;
                try object.put(context.arena, field.path, try context.storageDatum(datum, field));
                if (field.type == .json and !datum.sql_null and datum.value == .null) try json_null_fields.append(context.arena, field.path);
            };
            try mutations.append(context.arena, .{ .key = key, .expected_version = version, .expected_content_digest = digest, .row = if (bound.deleting) null else .{ .object = object }, .json_null_fields = json_null_fields.items, .previous = previous });
        }
    }
    return context.commitMutations(table, mutations.items, if (bound.deleting) "DELETE" else "UPDATE", bound.returning);
}
