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
pub fn presenceDirectory(alloc: Allocator, encoded: []const u8) !std.StringHashMapUnmanaged(void) {
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
    returning_plan: ?@import("mutation_returning.zig").Plan = null,
    returning_scope: []const @import("relation_binding.zig").Column = &.{},
    returning_sources: []const usize = &.{},
    target_qualifier: []const u8 = "",
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
    var query: ast.Select = .{ .source = source, .ctes = if (deleting) compiled.statement.delete.ctes else compiled.statement.update.ctes, .columns = projections.items, .predicate = predicate, .group_by = groups };
    // The target was already resolved/authorized for read+write. Reuse that
    // immutable binding; resolve every other physical source for read access.
    var adapter: @import("relation_binding.zig").TargetResolveAdapter = .{ .backend = backend, .table = table, .name = name, .cache_sources = true };
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
    var returning_plan: ?@import("mutation_returning.zig").Plan = null;
    var scope: []const @import("relation_binding.zig").Column = &.{};
    var captured: std.ArrayList(usize) = .empty;
    if (returning) |projections_| {
        const relation = input.relation orelse return error.InvalidSqlBackendResponse;
        for (projections_) |projection| {
            if (projection.wildcard) continue;
            const field: ast.Scalar = .{ .column = projection.field };
            _ = try @import("relation_binding.zig").lowerBoundExpression(alloc, relation.root.columns, projection.expression orelse &field);
        }
        if (!describe.returningReads(returning)) {
            scope = relation.root.columns;
            const slots = try alloc.alloc(scalar.Column, scope.len);
            for (scope, slots) |field, *slot| slot.* = .{ .name = field.internal, .type = field.type, .element_type = field.element_type };
            const plan = try @import("mutation_returning.zig").bind(alloc, backend, scope, slots, projections_, alias, parameters);
            const authorized_scope = scope;
            var required = try alloc.alloc(bool, scope.len);
            @memset(required, false);
            for (plan.programs) |program| for (program.required_columns) |ordinal| {
                required[ordinal] = true;
            };
            // Keep execution width proportional to RETURNING dependencies,
            // not to all columns in every authorized joined table. The full
            // scope remains authoritative for ambiguity and wildcard binding.
            var dense_scope: std.ArrayList(@import("relation_binding.zig").Column) = .empty;
            var dense_slots: std.ArrayList(scalar.Column) = .empty;
            var source_ordinals: std.ArrayList(usize) = .empty;
            for (scope, slots, required, 0..) |field, slot, used, ordinal| {
                if (!used) continue;
                try dense_scope.append(alloc, field);
                try dense_slots.append(alloc, slot);
                try source_ordinals.append(alloc, ordinal);
            }
            scope = dense_scope.items;
            returning_plan = try @import("mutation_returning.zig").bind(alloc, backend, authorized_scope, dense_slots.items, projections_, alias, parameters);
            required = try alloc.alloc(bool, scope.len);
            @memset(required, true);
            if (deleting) {
                var retained: std.ArrayList(ast.Projection) = .empty;
                try retained.appendSlice(alloc, projections.items[0..5]);
                var retained_fields: std.ArrayList(catalog.Column) = .empty;
                var retained_presence: std.ArrayList(bool) = .empty;
                for (fields.items, preserve.items, 0..) |field, preserved, index| {
                    const needed = for (scope, required) |visible, used| {
                        if (used and std.mem.eql(u8, visible.qualifier, alias) and std.mem.eql(u8, visible.name, field.name)) break true;
                    } else false;
                    if (!needed) continue;
                    try retained.append(alloc, projections.items[5 + index]);
                    try retained_fields.append(alloc, field);
                    try retained_presence.append(alloc, preserved);
                }
                projections = retained;
                fields = retained_fields;
                preserve = retained_presence;
            }
            for (scope, required, 0..) |field, needed, ordinal| {
                if (!needed or std.mem.eql(u8, field.qualifier, alias)) continue;
                try captured.append(alloc, ordinal);
                try projections.append(alloc, .{ .bound_column = source_ordinals.items[ordinal] });
            }
            if (captured.items.len != 0 or deleting) {
                query.columns = projections.items;
                // Source fanout is reduced by target identity in the streaming
                // collector, retaining one coherent representative source row.
                // Grouping by source cells would reintroduce fanout amplification.
                if (deleting) {
                    if (captured.items.len != 0) {
                        query.group_by = &.{};
                    } else {
                        const keys = try alloc.alloc(*const ast.Scalar, projections.items.len);
                        for (projections.items, keys) |projection, *key| key.* = projection.expression.?;
                        query.group_by = keys;
                    }
                }
                const expanded: compiler.Compiled = .{ .arena = undefined, .statement = .{ .select = query }, .parameter_count = compiled.parameter_count };
                input.* = try describe.bind(alloc, adapter.iface(), &expanded, parameters);
                const rebound = input.relation orelse return error.InvalidSqlBackendResponse;
                if (rebound.root.columns.len != authorized_scope.len) return error.InvalidSqlBackendResponse;
                for (authorized_scope, rebound.root.columns) |before, after| if (!std.mem.eql(u8, before.internal, after.internal) or before.type != after.type or before.element_type != after.element_type) return error.InvalidSqlBackendResponse;
            }
            input.parameter_types = parameters;
        }
    }
    return .{ .input = input, .query = query, .fields = fields.items, .preserve = preserve.items, .default_paths = default_paths.items, .deleting = deleting, .returning = returning, .returning_plan = returning_plan, .returning_scope = scope, .returning_sources = captured.items, .target_qualifier = alias };
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

pub fn execute(execution: anytype, definition_: Bound) !runtime.Output {
    var read = execution;
    read.binding = definition_.input.*;
    read.typed_output = true;
    // Match fanout is work, not the affected-row count. Both UPDATE FROM and
    // DELETE USING choose one coherent source match for each target, as in
    // PostgreSQL; the collector enforces the mutation quota on unique targets.
    read.limits.result_rows = execution.limits.scan_rows;
    var pending_mutations: std.ArrayList(catalog.Mutation) = .empty;
    defer pending_mutations.deinit(execution.alloc);
    var visited: std.StringHashMapUnmanaged(void) = .empty;
    defer visited.deinit(execution.alloc);
    const target = execution.binding.table.?;
    var previous_layout: ?catalog.Row.TypedLayout = null;
    if (definition_.deleting and definition_.returning != null) {
        const names = try execution.arena.alloc([]const u8, definition_.fields.len);
        for (definition_.fields, names) |field, *name| name.* = field.path;
        previous_layout = try catalog.Row.TypedLayout.init(execution.arena, names);
    }
    const Capture = @import("result_cursor.zig").Cursor;
    const sources = if (definition_.returning_sources.len == 0) null else if (execution.spill) |manager| try Capture.create(execution.alloc, manager, definition_.returning_sources.len) else try Capture.createMemory(execution.alloc, definition_.returning_sources.len, execution.limits.retained_bytes / 2);
    var sources_live = sources != null;
    defer if (sources_live) sources.?.close();
    const Collector = struct {
        context: runtime.Context,
        bound: Bound,
        mutations: *std.ArrayList(catalog.Mutation),
        seen: *std.StringHashMapUnmanaged(void),
        old_layout: ?catalog.Row.TypedLayout,
        sources: ?*Capture,
        scratch: std.heap.ArenaAllocator,
        fn append(raw: *anyopaque, values: []const scalar.Datum) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const context_ = self.context;
            const bound_ = self.bound;
            return self.add(context_, bound_, values);
        }
        fn add(self: *@This(), ctx: runtime.Context, definition: Bound, values: []const scalar.Datum) !void {
            const context = ctx;
            const bound = definition;
            const table = context.binding.table.?;
            const mutations = self.mutations;
            const seen = self.seen;
            const old_layout = self.old_layout;
            _ = self.scratch.reset(.retain_capacity);
            const temporary = self.scratch.allocator();
            try context.checkpoint();
            if (values.len != bound.fields.len + 5 + bound.returning_sources.len) return error.InvalidSqlBackendResponse;
            if (values[0].sql_null) return; // An outer join may have no target row.
            if (values[0].value != .string or values[1].sql_null or values[1].value != .string or values[2].sql_null or values[2].value != .string) return error.InvalidSqlBackendResponse;
            if (seen.contains(values[0].value.string)) {
                return;
            }
            if (mutations.items.len >= context.limits.mutation_rows) return error.SqlProgramLimitExceeded;
            const key = try context.arena.dupe(u8, values[0].value.string);
            try seen.put(context.alloc, key, {});
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
                old.* = try catalog.Row.fromDatums(context.arena, key, old_layout.?, values[5..][0..bound.fields.len]);
                old.version = version;
                old.expected_content_digest = digest;
                if (!values[3].sql_null) old.document = try runtime.clone(context.arena, values[3].value);
                const presence = try context.arena.alloc(bool, bound.fields.len);
                for (bound.fields, presence) |field, *flag| flag.* = present.contains(field.name);
                old.typed_cells.?.presence = presence;
                break :blk old;
            } else null;
            if (!bound.deleting) for (bound.fields, bound.preserve, values[5..][0..bound.fields.len]) |field, preserve, datum| {
                if (preserve and !present.contains(field.name)) continue;
                try object.put(context.arena, field.path, try context.storageDatum(datum, field));
                if (field.type == .json and !datum.sql_null and datum.value == .null) try json_null_fields.append(context.arena, field.path);
            };
            try mutations.append(context.alloc, .{ .key = key, .expected_version = version, .expected_content_digest = digest, .row = if (bound.deleting) null else .{ .object = object }, .json_null_fields = json_null_fields.items, .previous = previous });
            if (self.sources) |capture| try Capture.append(capture, values[5 + bound.fields.len ..]);
        }
    };
    var collector: Collector = .{ .context = execution, .bound = definition_, .mutations = &pending_mutations, .seen = &visited, .old_layout = previous_layout, .sources = sources, .scratch = std.heap.ArenaAllocator.init(execution.alloc) };
    defer collector.scratch.deinit();
    // Consume the statement's one captured read directly. DELETE fanout keeps
    // one target image and one coherent source representative, not a full join
    // result per match. Only required source RETURNING cells enter the bounded
    // typed capture; no per-row JSON compatibility representation is retained.
    try read.selectInto(definition_.query, .{ .ptr = &collector, .append = Collector.append });
    if (definition_.returning_plan) |plan| {
        const output = try returningOutput(execution, definition_, plan, pending_mutations.items, sources);
        if (sources) |capture| capture.close();
        sources_live = false;
        var committed = try execution.commitPreparedMutations(target, output.prepared, if (definition_.deleting) "DELETE" else "UPDATE");
        committed.columns = plan.columns;
        committed.rows = output.rows;
        committed.sql_nulls = output.nulls;
        return committed;
    }
    return execution.commitMutations(target, pending_mutations.items, if (definition_.deleting) "DELETE" else "UPDATE", definition_.returning);
}

const ReturningOutput = struct { prepared: []const catalog.Mutation, rows: []const []const std.json.Value, nulls: []const []const bool };

fn returningOutput(context: runtime.Context, bound: Bound, plan: @import("mutation_returning.zig").Plan, input: []const catalog.Mutation, sources: ?*@import("result_cursor.zig").Cursor) !ReturningOutput {
    if (input.len > context.limits.result_rows) return error.SqlResultTooLarge;
    const prepared = if (input.len == 0) input else try (context.backend.vtable.prepare_mutations orelse return error.UnsupportedSqlExecution)(context.backend.ptr, context.arena, context.binding.table.?, input);
    if (prepared.len != input.len) return error.InvalidSqlBackendResponse;
    const required = try context.arena.alloc(bool, bound.returning_scope.len);
    @memset(required, false);
    for (plan.programs) |program| for (program.required_columns) |ordinal| {
        required[ordinal] = true;
    };
    var fields: std.ArrayList([]const u8) = .empty;
    for (bound.returning_scope, required) |field, needed| if (needed and std.mem.eql(u8, field.qualifier, bound.target_qualifier)) {
        try fields.append(context.arena, field.name);
    };
    const images = try runtime.Context.ReturningImages.init(context.arena, context.binding.table.?, fields.items);
    const rows = try context.arena.alloc([]const std.json.Value, input.len);
    const nulls = try context.arena.alloc([]const bool, input.len);
    const reader = if (sources) |capture| try capture.openReplayReader() else null;
    defer if (reader) |cursor| cursor.close();
    var arena = std.heap.ArenaAllocator.init(context.alloc);
    defer arena.deinit();
    const decisions = @import("decision_eval.zig");
    var first: usize = 0;
    while (first < input.len) {
        try context.checkpoint();
        _ = arena.reset(.retain_capacity);
        const a = arena.allocator();
        var budget: decisions.PageBudget = .{ .row_limit = context.limits.page_rows, .byte_limit = context.limits.page_bytes };
        var page: std.ArrayList([]const scalar.Datum) = .empty;
        var end = first;
        while (end < input.len) {
            const image = try images.row(a, prepared[end], input[end]);
            const cells = try a.alloc(scalar.Datum, bound.returning_scope.len);
            @memset(cells, .{});
            if (reader) |cursor| {
                const source = (try cursor.next()) orelse return error.InvalidSqlBackendResponse;
                if (source.len != bound.returning_sources.len) return error.InvalidSqlBackendResponse;
                for (source, bound.returning_sources) |value, ordinal| cells[ordinal] = try @import("operators.zig").cloneDatum(a, value);
            }
            for (bound.returning_scope, required, cells) |field, needed, *cell_| if (needed and std.mem.eql(u8, field.qualifier, bound.target_qualifier)) {
                cell_.* = try image.cell(field.name);
            };
            const full = try budget.add(cells);
            try page.append(a, cells);
            end += 1;
            if (full) break;
        }
        const evaluated = try a.alloc([]const scalar.Datum, plan.programs.len);
        for (plan.programs, evaluated) |*program, *column_| column_.* = try decisions.evaluateBatch(a, context.backend.decision_provider, program, page.items, context.parameters);
        for (first..end) |index| {
            const values = try context.arena.alloc(std.json.Value, plan.programs.len);
            const flags = try context.arena.alloc(bool, plan.programs.len);
            for (evaluated, plan.columns, values, flags) |column_values, descriptor, *value, *flag| {
                const datum = column_values[index - first];
                value.* = try context.outputDatum(datum, descriptor.type, descriptor.element_type);
                flag.* = datum.sql_null;
            }
            rows[index] = values;
            nulls[index] = flags;
        }
        first = end;
    }
    if (reader) |cursor| if (cursor.index != input.len or cursor.owner.count() != input.len) return error.InvalidSqlBackendResponse;
    return .{ .prepared = prepared, .rows = rows, .nulls = nulls };
}
