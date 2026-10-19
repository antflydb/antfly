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

//! Pure schema changes. Caller owns the arena and commits the result with the
//! exact schema version captured from the native catalog.
const std = @import("std");
const ast = @import("ast.zig");
const Value = std.json.Value;

test {
    _ = @import("schema_expression.zig");
    _ = @import("../schema/column_nullability.zig");
}

fn value(alloc: std.mem.Allocator, input: anytype) !Value {
    return std.json.parseFromSliceLeaky(Value, alloc, try std.json.Stringify.valueAlloc(alloc, input, .{}), .{ .parse_numbers = false });
}
fn list(schema: *Value, alloc: std.mem.Allocator, key: []const u8) !*std.array_list.Managed(Value) {
    const entry = try schema.object.getOrPut(alloc, key);
    if (!entry.found_existing) entry.value_ptr.* = .{ .array = std.array_list.Managed(Value).init(alloc) };
    if (entry.value_ptr.* != .array) return error.InvalidSqlBackendResponse;
    return &entry.value_ptr.array;
}
fn named(items: []const Value, name: []const u8, key: []const u8) ?usize {
    for (items, 0..) |item, i| {
        if (item != .object) continue;
        const item_name = item.object.get(key) orelse continue;
        if (item_name == .string and std.mem.eql(u8, item_name.string, name)) return i;
    }
    return null;
}

fn constraintExists(schema: Value, name: []const u8) bool {
    for ([_][]const u8{ "unique_constraints", "checks", "foreign_keys" }) |key| {
        const entries = schema.object.get(key) orelse continue;
        if (entries == .array and named(entries.array.items, name, "name") != null) return true;
    }
    return false;
}

fn indexOwned(unique: Value) bool {
    if (unique != .object) return false;
    const origin = unique.object.get("origin") orelse return false;
    return origin == .string and std.mem.eql(u8, origin.string, "index");
}

fn indexOwnershipNamespaces(allocator: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var schema = try value(a, .{ .storage_mode = "relational", .default_type = "row", .document_schemas = .{ .row = .{ .schema = .{
        .type = "object",
        .properties = .{ .id = .{ .type = "integer" }, .email = .{ .type = "keyword" }, .tenant_id = .{ .type = "keyword" }, .status = .{ .type = "keyword" } },
        .additionalProperties = false,
    } } } });
    const compiler = @import("compiler.zig");
    for ([_][]const u8{
        "ALTER TABLE items ADD CONSTRAINT named_id UNIQUE (id)",
        "CREATE UNIQUE INDEX email_key ON items (email)",
        "CREATE UNIQUE INDEX partial_email ON items (email) WHERE status='active'",
        "CREATE UNIQUE INDEX folded_email ON items (lower(email))",
        "CREATE UNIQUE INDEX tenant_folded_email ON items (tenant_id,lower(email))",
    }) |sql| {
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        try std.testing.expect(try apply(a, &schema, compiled.statement.catalog_ddl));
    }
    for (schema.object.getPtr("relational_indexes").?.array.items) |*index| {
        // Descriptions are editable display metadata, not ownership evidence.
        try index.object.put(a, "description", .{ .string = "edited by operator" });
        const name = index.object.get("name").?.string;
        const uniques = schema.object.get("unique_constraints").?.array.items;
        try std.testing.expect(indexOwned(uniques[named(uniques, name, "name").?]));
        for ([_][]const u8{ "DROP", "VALIDATE" }) |action| {
            const before = try std.json.Stringify.valueAlloc(a, schema, .{});
            var compiled = try compiler.compile(a, try std.fmt.allocPrint(a, "ALTER TABLE items {s} CONSTRAINT {s}", .{ action, name }), .{});
            defer compiled.deinit();
            if (apply(a, &schema, compiled.statement.catalog_ddl)) |_| {
                return error.ExpectedConstraintNotFound;
            } else |err| {
                // Allocation fault enumeration must observe injected OOM,
                // including failures while staging an otherwise invalid DDL.
                if (err == error.OutOfMemory) return err;
                try std.testing.expectEqual(error.SqlConstraintNotFound, err);
            }
            try std.testing.expectEqualStrings(before, try std.json.Stringify.valueAlloc(a, schema, .{}));
        }
    }
    // The namespace publisher consumes the same canonical definitions as
    // storage, not a second SQL-name interpretation or display-label parser.
    var cut = try @import("../system_catalog/relation_names.zig").TableCut.init(allocator, .{
        .namespace_id = 2,
        .table_id = 7,
        .name = "items",
        .schema_json = try std.json.Stringify.valueAlloc(a, schema, .{}),
    });
    defer cut.deinit();
    try std.testing.expectEqual(@as(usize, 6), cut.claims.len);
    for (cut.claims) |claim| {
        const expected: @import("../system_catalog/relation_names.zig").Kind = if (std.mem.eql(u8, claim.key.name, "items")) .table else if (std.mem.eql(u8, claim.key.name, "named_id")) .constraint_index else .index;
        try std.testing.expectEqual(expected, claim.owner.kind);
    }
    for ([_][]const u8{ "email_key", "partial_email", "folded_email", "tenant_folded_email" }) |name| {
        // This is the table-bound schema transition. PostgreSQL's unqualified
        // DROP INDEX additionally needs namespace/catalog owner resolution.
        try std.testing.expect(try apply(a, &schema, .{ .kind = .table, .action = .alter_schema, .name = .{ .table = "items" }, .schema_change = .{ .drop_index = name } }));
        try std.testing.expect(named(schema.object.get("unique_constraints").?.array.items, name, "name") == null);
    }
    try std.testing.expectEqual(@as(usize, 1), schema.object.get("unique_constraints").?.array.items.len);
    // A spoofed legacy display label must not manufacture a paired owner.
    var ordinary = try compiler.compile(a, "CREATE INDEX display_only ON items (id)", .{});
    defer ordinary.deinit();
    try std.testing.expect(try apply(a, &schema, ordinary.statement.catalog_ddl));
    try schema.object.getPtr("relational_indexes").?.array.items[0].object.put(a, "description", .{ .string = "SQL UNIQUE INDEX" });
    try std.testing.expect(try apply(a, &schema, .{ .kind = .table, .action = .alter_schema, .name = .{ .table = "items" }, .schema_change = .{ .drop_index = "display_only" } }));
    try std.testing.expectEqualStrings("named_id", schema.object.get("unique_constraints").?.array.items[0].object.get("name").?.string);
}

test "SQL unique index ownership is independent of named constraint and display namespaces" {
    try indexOwnershipNamespaces(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, indexOwnershipNamespaces, .{});
}

fn hasPrimary(schema: Value) bool {
    const constraints = schema.object.get("unique_constraints") orelse return false;
    if (constraints != .array) return false;
    for (constraints.array.items) |constraint| {
        if (constraint != .object) continue;
        const primary = constraint.object.get("primary") orelse continue;
        if (primary == .bool and primary.bool) return true;
    }
    return false;
}

fn requirePrimaryColumns(alloc: std.mem.Allocator, schema: *Value, columns: []const []const u8) !void {
    const default_type = schema.object.get("default_type") orelse return error.InvalidSqlBackendResponse;
    if (default_type != .string) return error.InvalidSqlBackendResponse;
    const document_schemas = schema.object.getPtr("document_schemas") orelse return error.InvalidSqlBackendResponse;
    if (document_schemas.* != .object) return error.InvalidSqlBackendResponse;
    const document = document_schemas.object.getPtr(default_type.string) orelse return error.InvalidSqlBackendResponse;
    if (document.* != .object) return error.InvalidSqlBackendResponse;
    const row = document.object.getPtr("schema") orelse return error.InvalidSqlBackendResponse;
    if (row.* != .object) return error.InvalidSqlBackendResponse;
    // Adding required can relocate row slots, but not this nested map's data.
    const properties = row.object.get("properties") orelse return error.InvalidSqlBackendResponse;
    if (properties != .object) return error.InvalidSqlBackendResponse;
    if (columns.len == 0) return error.InvalidSqlSyntax;
    // Validate the entire key before changing either the column shape or its
    // uniqueness declaration. A failed composite key cannot leave a prefix
    // of columns marked NOT NULL in the caller's candidate schema.
    for (columns, 0..) |column, index| {
        if (std.mem.eql(u8, column, "_id")) return error.UnsupportedSqlShape;
        const property = properties.object.get(column) orelse return error.UndefinedColumn;
        if (property != .object) return error.InvalidSqlBackendResponse;
        for (columns[0..index]) |prior| if (std.mem.eql(u8, prior, column)) return error.DuplicateSqlColumn;
    }
    for (columns) |column| {
        _ = try setColumnNullability(alloc, schema.*, row, properties, column, false);
    }
}

const setColumnNullability = @import("../schema/column_nullability.zig").setColumn;

pub fn apply(alloc: std.mem.Allocator, schema: *Value, ddl: ast.CatalogDdl) !bool {
    return applyWithLimits(alloc, schema, ddl, .{});
}

pub fn applyWithLimits(alloc: std.mem.Allocator, schema: *Value, ddl: ast.CatalogDdl, limits: @import("immutable_plan.zig").Limits) !bool {
    if (limits.checkpoint) |check| try check(limits.checkpoint_context);
    var staging = std.heap.ArenaAllocator.init(alloc);
    defer staging.deinit();
    var candidate = try value(staging.allocator(), schema.*);
    const changed = try applyCandidateWithLimits(staging.allocator(), &candidate, ddl, limits);
    // Candidate allocations, including failed binding scratch, are never
    // retained by the caller. Successful result storage belongs to its schema
    // arena, as do the other JSON schema-builder operations in this module.
    if (limits.checkpoint) |check| try check(limits.checkpoint_context);
    if (changed) schema.* = try value(alloc, candidate);
    return changed;
}

/// Owned unpublished schema and single-rule fragment for catalog preparation.
pub const PreparedAddition = struct {
    schema: Value,
    fragment_json: []const u8,
    generated_name: []const u8,
    key: []const u8,
    selected: bool = false,

    /// Replace only the already-lowered addition, never rebind an expression
    /// after metadata has chosen a name. Storage belongs to the caller's arena.
    pub fn selectName(self: *PreparedAddition, alloc: std.mem.Allocator, name: []const u8) !void {
        if (self.selected) return error.InvalidSqlBackendResponse;
        if (name.len == 0 or name.len > 63 or !std.unicode.utf8ValidateSlice(name) or std.mem.indexOfScalar(u8, name, 0) != null)
            return error.InvalidSqlBackendResponse;
        const entries = self.schema.object.getPtr(self.key) orelse return error.InvalidSqlBackendResponse;
        if (entries.* != .array or entries.array.items.len == 0) return error.InvalidSqlBackendResponse;
        const added = &entries.array.items[entries.array.items.len - 1];
        if (added.* != .object) return error.InvalidSqlBackendResponse;
        const field = added.object.getPtr("name") orelse return error.InvalidSqlBackendResponse;
        if (field.* != .string or !std.mem.eql(u8, field.string, self.generated_name)) return error.InvalidSqlBackendResponse;
        field.* = .{ .string = try alloc.dupe(u8, name) };
        self.selected = true;
    }
};

/// Anonymous ALTER uses a collision-free temporary label for local validation.
/// Only the catalog-selected name may be published. A failed preparation never
/// mutates the predecessor; successful storage belongs to the caller's arena.
pub fn prepareGeneratedAddition(alloc: std.mem.Allocator, schema: Value, ddl: ast.CatalogDdl) !?PreparedAddition {
    const original = ddl.schema_change orelse return null;
    _ = original.generatedConstraintName() orelse return null;
    if (schema != .object) return error.InvalidSqlBackendResponse;
    // One hash-table seed avoids quadratic suffix probing over a large schema.
    // Include index-owned rules even when public naming permits their overlap.
    var occupied = std.StringHashMapUnmanaged(void).empty;
    defer occupied.deinit(alloc);
    var count: usize = 0;
    for ([_][]const u8{ "unique_constraints", "checks", "foreign_keys", "relational_indexes" }) |key| {
        const entries = schema.object.get(key) orelse continue;
        if (entries != .array) return error.InvalidSqlBackendResponse;
        for (entries.array.items) |entry| {
            if (count == 8192) return error.ProgramLimitExceeded;
            count += 1;
            if (entry != .object) return error.InvalidSqlBackendResponse;
            const name = entry.object.get("name") orelse return error.InvalidSqlBackendResponse;
            if (name != .string) return error.InvalidSqlBackendResponse;
            try occupied.put(alloc, name.string, {});
        }
    }
    var scratch: [63]u8 = undefined;
    var suffix: usize = 0;
    const label = while (suffix <= count) : (suffix += 1) {
        const name = try std.fmt.bufPrint(&scratch, "__antfly_unpublished_{d}", .{suffix});
        if (!occupied.contains(name)) break try alloc.dupe(u8, name);
    } else return error.ProgramLimitExceeded;
    var staged = ddl;
    const key: []const u8 = switch (staged.schema_change.?) {
        .add_unique => |*rule| blk: {
            rule.name = label;
            break :blk "unique_constraints";
        },
        .add_check => |*rule| blk: {
            rule.name = label;
            break :blk "checks";
        },
        .add_foreign_key => |*rule| blk: {
            rule.name = label;
            break :blk "foreign_keys";
        },
        else => unreachable,
    };
    var candidate = try value(alloc, schema);
    if (!try applyCandidate(alloc, &candidate, staged)) return error.InvalidSqlBackendResponse;
    const entries = candidate.object.get(key).?.array.items;
    var fragment: Value = .{ .object = .empty };
    var additions = std.json.Array.init(alloc);
    try additions.append(entries[entries.len - 1]);
    try fragment.object.put(alloc, key, .{ .array = additions });
    return .{
        .schema = candidate,
        .fragment_json = try std.json.Stringify.valueAlloc(alloc, fragment, .{}),
        .generated_name = label,
        .key = key,
    };
}

fn generatedAdditionCase(allocator: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const baseline = try value(a, .{ .storage_mode = "relational", .default_type = "row", .document_schemas = .{ .row = .{ .schema = .{
        .type = "object",
        .properties = .{ .id = .{ .type = "integer" } },
        .additionalProperties = false,
    } } }, .checks = .{ .{ .name = "items_id_check", .expression = .{ .op = "literal", .value = true } }, .{ .name = "__antfly_unpublished_0", .expression = .{ .op = "literal", .value = true } } } });
    const before = try std.json.Stringify.valueAlloc(a, baseline, .{});
    for ([_][]const u8{
        "ALTER TABLE items ADD CHECK (id > 0)",
        "ALTER TABLE items ADD UNIQUE (id)",
        "ALTER TABLE items ADD PRIMARY KEY (id)",
        "ALTER TABLE items ADD FOREIGN KEY (id) REFERENCES parents(id)",
    }) |sql| {
        var compiled = try @import("compiler.zig").compile(a, sql, .{});
        defer compiled.deinit();
        var prepared = (try prepareGeneratedAddition(a, baseline, compiled.statement.catalog_ddl)).?;
        try std.testing.expectEqualStrings("__antfly_unpublished_1", prepared.generated_name);
        const fragment = try std.json.parseFromSliceLeaky(Value, a, prepared.fragment_json, .{});
        try std.testing.expectEqual(@as(usize, 1), fragment.object.count());
        try std.testing.expectEqual(@as(usize, 1), fragment.object.get(prepared.key).?.array.items.len);
        try std.testing.expectEqualStrings(prepared.generated_name, fragment.object.get(prepared.key).?.array.items[0].object.get("name").?.string);
        for ([_][]const u8{ "", "bad\x00name", "\xff", "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl" }) |invalid| {
            try std.testing.expectError(error.InvalidSqlBackendResponse, prepared.selectName(a, invalid));
            try std.testing.expect(!prepared.selected);
        }
        try prepared.selectName(a, "chosen_name1");
        const entries = prepared.schema.object.get(prepared.key).?.array.items;
        try std.testing.expectEqualStrings("chosen_name1", entries[entries.len - 1].object.get("name").?.string);
        try std.testing.expectError(error.InvalidSqlBackendResponse, prepared.selectName(a, "chosen_name2"));
        try std.testing.expectEqualStrings(before, try std.json.Stringify.valueAlloc(a, baseline, .{}));
    }
    var explicit = try @import("compiler.zig").compile(a, "ALTER TABLE items ADD CONSTRAINT items_id_check CHECK (id > 0)", .{});
    defer explicit.deinit();
    try std.testing.expect((try prepareGeneratedAddition(a, baseline, explicit.statement.catalog_ddl)) == null);
    var unchanged = baseline;
    if (apply(a, &unchanged, explicit.statement.catalog_ddl)) |_| return error.TestUnexpectedError else |err| {
        if (err == error.OutOfMemory) return err;
        try std.testing.expectEqual(error.SqlConstraintAlreadyExists, err);
    }
    try std.testing.expectEqualStrings(before, try std.json.Stringify.valueAlloc(a, unchanged, .{}));
    var primary = baseline;
    var named_primary = try @import("compiler.zig").compile(a, "ALTER TABLE items ADD CONSTRAINT existing_pk PRIMARY KEY (id)", .{});
    defer named_primary.deinit();
    _ = try apply(a, &primary, named_primary.statement.catalog_ddl);
    const primary_before = try std.json.Stringify.valueAlloc(a, primary, .{});
    var duplicate_primary = try @import("compiler.zig").compile(a, "ALTER TABLE items ADD PRIMARY KEY (id)", .{});
    defer duplicate_primary.deinit();
    if (prepareGeneratedAddition(a, primary, duplicate_primary.statement.catalog_ddl)) |_| return error.TestUnexpectedError else |err| {
        if (err == error.OutOfMemory) return err;
        try std.testing.expectEqual(error.SqlConstraintAlreadyExists, err);
    }
    try std.testing.expectEqualStrings(primary_before, try std.json.Stringify.valueAlloc(a, primary, .{}));
}

test "generated addition stages a collision-free label and renames without relowering" {
    try generatedAdditionCase(std.testing.allocator);
}

test "generated addition owns candidate storage through allocation failures" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, generatedAdditionCase, .{});
}

test "generated addition bounds schema seeding and rejects malformed ownership" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var compiled = try @import("compiler.zig").compile(a, "ALTER TABLE items ADD CHECK (true)", .{});
    defer compiled.deinit();
    var malformed = try value(a, .{ .checks = .{.{ .name = 7 }} });
    try std.testing.expectError(error.InvalidSqlBackendResponse, prepareGeneratedAddition(a, malformed, compiled.statement.catalog_ddl));
    malformed = try value(a, .{ .checks = true });
    try std.testing.expectError(error.InvalidSqlBackendResponse, prepareGeneratedAddition(a, malformed, compiled.statement.catalog_ddl));
    var entries = std.json.Array.init(a);
    const entry = try value(a, .{ .name = "same_label" });
    for (0..8193) |_| try entries.append(entry);
    var oversized: Value = .{ .object = .empty };
    try oversized.object.put(a, "checks", .{ .array = entries });
    // Count records, not distinct names: duplicates cannot evade the envelope.
    try std.testing.expectError(error.ProgramLimitExceeded, prepareGeneratedAddition(a, oversized, compiled.statement.catalog_ddl));
}

/// In-place builder for an already unpublished schema, allowing CREATE's
/// constraint batch to avoid cloning the entire schema for every constraint.
pub fn applyCandidate(alloc: std.mem.Allocator, schema: *Value, ddl: ast.CatalogDdl) !bool {
    return applyCandidateWithLimits(alloc, schema, ddl, .{});
}

pub fn applyCandidateWithLimits(alloc: std.mem.Allocator, schema: *Value, ddl: ast.CatalogDdl, limits: @import("immutable_plan.zig").Limits) !bool {
    if (limits.checkpoint) |check| try check(limits.checkpoint_context);
    if (ddl.kind == .index and ddl.index_targets.len > 1) return error.UnsupportedSqlShape;
    const change = ddl.schema_change orelse return error.InvalidSqlSyntax;
    if (schema.* != .object) return error.InvalidSqlBackendResponse;
    switch (change) {
        .drop_constraint, .validate_constraint => |constraint_name| {
            for ([_][]const u8{ "unique_constraints", "checks", "foreign_keys" }) |key| {
                const constraints = try list(schema, alloc, key);
                if (named(constraints.items, constraint_name, "name")) |position| {
                    if (std.mem.eql(u8, key, "unique_constraints") and indexOwned(constraints.items[position])) return error.SqlConstraintNotFound;
                    // PostgreSQL VALIDATE applies only to CHECK and FK rules.
                    // UNIQUE/PRIMARY ownership is enforced at activation; a
                    // validation request must not silently publish a new epoch.
                    if (change == .validate_constraint and std.mem.eql(u8, key, "unique_constraints")) return error.SqlWrongObjectType;
                    if (change == .drop_constraint) {
                        _ = constraints.orderedRemove(position);
                    } else if (std.mem.eql(u8, key, "checks")) {
                        // The caller must validate this unpublished candidate
                        // before publishing its cleared declaration. Background
                        // coverage alone never changes declared SQL validity.
                        try constraints.items[position].object.put(alloc, "not_valid", .{ .bool = false });
                    }
                    return true;
                }
            }
            return error.SqlConstraintNotFound;
        },
        .add_unique => |constraint| {
            if (constraintExists(schema.*, constraint.name)) return error.SqlConstraintAlreadyExists;
            if (constraint.primary and hasPrimary(schema.*)) return error.SqlConstraintAlreadyExists;
            if (constraint.primary and (constraint.deferrable or !std.mem.eql(u8, constraint.timing, "immediate"))) return error.UnsupportedSqlShape;
            if (constraint.primary) try requirePrimaryColumns(alloc, schema, constraint.columns);
            const constraints = try list(schema, alloc, "unique_constraints");
            if (named(constraints.items, constraint.name, "name") != null) return error.SqlConstraintAlreadyExists;
            try constraints.append(try value(alloc, .{ .name = constraint.name, .columns = constraint.columns, .primary = constraint.primary, .deferrable = constraint.deferrable, .timing = constraint.timing, .nulls_not_distinct = constraint.nulls_not_distinct }));
        },
        .add_check => |constraint| {
            if (constraintExists(schema.*, constraint.name)) return error.SqlConstraintAlreadyExists;
            const expression = try @import("schema_expression.zig").lower(alloc, schema.*, constraint.expression, .boolean);
            const constraints = try list(schema, alloc, "checks");
            if (named(constraints.items, constraint.name, "name") != null) return error.SqlConstraintAlreadyExists;
            try constraints.append(try value(alloc, .{ .name = constraint.name, .expression = expression, .not_valid = constraint.not_valid }));
        },
        .add_foreign_key => |constraint| {
            if (constraintExists(schema.*, constraint.name)) return error.SqlConstraintAlreadyExists;
            const constraints = try list(schema, alloc, "foreign_keys");
            if (named(constraints.items, constraint.name, "name") != null) return error.SqlConstraintAlreadyExists;
            try constraints.append(try value(alloc, .{ .name = constraint.name, .child_columns = constraint.columns, .parent_table = constraint.parent, .parent_columns = constraint.parent_columns, .on_delete = constraint.on_delete, .on_update = constraint.on_update, .match = constraint.match, .timing = constraint.timing, .deferrable = constraint.deferrable }));
        },
        .create_index => |index| {
            var indexes = try list(schema, alloc, "relational_indexes");
            if (named(indexes.items, index.name, "name") != null) {
                if (ddl.conditional) return false;
                return error.SqlIndexAlreadyExists;
            }
            if (index.unique and constraintExists(schema.*, index.name)) return error.SqlConstraintAlreadyExists;
            var keys = std.ArrayList(Value).empty;
            var columns = std.ArrayList([]const u8).empty;
            for (index.keys) |key| {
                const nulls: []const u8 = if (key.nulls_first orelse key.descending) "first" else "last";
                if (key.expression) |expression| {
                    const lowered = try @import("schema_expression.zig").lowerIndexKey(alloc, schema.*, expression, limits);
                    try keys.append(alloc, try value(alloc, .{ .expression = lowered.expression, .result_type = if (lowered.element_type == .numeric) "numeric" else if (lowered.type == .uuid) "string" else @tagName(lowered.type), .direction = if (key.descending) "desc" else "asc", .nulls = nulls }));
                } else try keys.append(alloc, try value(alloc, .{ .column = key.field, .direction = if (key.descending) "desc" else "asc", .nulls = nulls }));
                try columns.append(alloc, key.field);
            }
            // Explicit unique-owner provenance, not the display description,
            // links this index to its rule. Both publish in one schema CAS.
            const predicates = if (index.predicate) |predicate| try @import("schema_expression.zig").lowerIndexPredicateWithLimits(alloc, schema.*, predicate, limits) else &.{};
            try indexes.append(try value(alloc, .{ .name = index.name, .keys = keys.items, .include_columns = index.include_columns, .where = predicates, .description = if (index.unique) "SQL UNIQUE INDEX" else "SQL INDEX" }));
            if (index.unique) {
                const constraints = try list(schema, alloc, "unique_constraints");
                if (named(constraints.items, index.name, "name") != null) return error.SqlConstraintAlreadyExists;
                const has_expression = for (index.keys) |key| {
                    if (key.expression != null) break true;
                } else false;
                try constraints.append(if (has_expression)
                    try value(alloc, .{ .name = index.name, .origin = "index", .keys = keys.items, .where = predicates, .nulls_not_distinct = index.nulls_not_distinct })
                else
                    try value(alloc, .{ .name = index.name, .origin = "index", .columns = columns.items, .where = predicates, .nulls_not_distinct = index.nulls_not_distinct }));
            }
        },
        .drop_index => |index_name| {
            const indexes = try list(schema, alloc, "relational_indexes");
            const position = named(indexes.items, index_name, "name") orelse {
                if (ddl.conditional) return false;
                return error.SqlIndexNotFound;
            };
            _ = indexes.orderedRemove(position);
            const constraints = try list(schema, alloc, "unique_constraints");
            if (named(constraints.items, index_name, "name")) |unique_position| if (indexOwned(constraints.items[unique_position])) {
                _ = constraints.orderedRemove(unique_position);
            };
        },
        else => {
            const default_type = schema.object.get("default_type") orelse return error.InvalidSqlBackendResponse;
            var document_schemas = schema.object.getPtr("document_schemas") orelse return error.InvalidSqlBackendResponse;
            var document = document_schemas.object.getPtr(default_type.string) orelse return error.InvalidSqlBackendResponse;
            var row = document.object.getPtr("schema") orelse return error.InvalidSqlBackendResponse;
            var properties = row.object.getPtr("properties") orelse return error.InvalidSqlBackendResponse;
            const column_name = switch (change) {
                .add_column => |c| c.name,
                .drop_column, .drop_default => |n| n,
                .set_default => |d| d.column,
                .set_nullability => |d| d.column,
                else => unreachable,
            };
            if (std.mem.eql(u8, column_name, "_id")) return error.UnsupportedSqlShape;
            if (change == .set_nullability)
                return setColumnNullability(alloc, schema.*, row, properties.*, column_name, change.set_nullability.nullable);
            if (change == .add_column) {
                if (properties.object.contains(column_name)) return error.DuplicateSqlColumn;
                const column = change.add_column;
                if (column.default_expression != null and column.generated_expression != null) return error.InvalidSqlSyntax;
                try properties.object.put(alloc, try alloc.dupe(u8, column_name), try @import("ddl_runtime.zig").columnProperty(alloc, column, column.nullable));
                if (!change.add_column.nullable) {
                    const required = try list(row, alloc, "required");
                    try required.append(.{ .string = try alloc.dupe(u8, column_name) });
                }
                if (column.default_expression) |expression| {
                    const defaults = try list(schema, alloc, "column_defaults");
                    const lowered = try @import("schema_expression.zig").lowerAssignmentWithLimits(alloc, schema.*, expression, column, false, limits);
                    try defaults.append(try value(alloc, .{ .column = column_name, .expression = lowered }));
                }
                if (column.generated_expression) |expression| {
                    const generated = try list(schema, alloc, "generated_columns");
                    const lowered = try @import("schema_expression.zig").lowerAssignmentWithLimits(alloc, schema.*, expression, column, true, limits);
                    try generated.append(try value(alloc, .{ .column = column_name, .expression = lowered }));
                }
            } else {
                const property = properties.object.get(column_name) orelse return error.UndefinedColumn;
                if (schema.object.getPtr("generated_columns")) |generated| {
                    if (generated.* != .array) return error.InvalidSqlBackendResponse;
                    if (named(generated.array.items, column_name, "column")) |i| {
                        if (change != .drop_column) return error.InvalidSqlSyntax;
                        _ = generated.array.orderedRemove(i);
                    }
                }
                if (change == .drop_column) {
                    _ = properties.object.swapRemove(column_name);
                    if (row.object.getPtr("required")) |required| {
                        for (required.array.items, 0..) |entry, i| if (entry == .string and std.mem.eql(u8, entry.string, column_name)) {
                            _ = required.array.orderedRemove(i);
                            break;
                        };
                    }
                }
                var replacement: ?Value = null;
                if (change == .set_default) {
                    const column = try @import("schema_columns.zig").column(column_name, property);
                    const element = column.element_type;
                    const column_type = column.type;
                    const expression = try @import("schema_expression.zig").lowerAssignmentWithLimits(alloc, schema.*, change.set_default.expression, .{ .name = column_name, .type = column_type, .element_type = element }, false, limits);
                    replacement = try value(alloc, .{ .column = column_name, .expression = expression });
                }
                const defaults = try list(schema, alloc, "column_defaults");
                if (replacement != null) try defaults.ensureUnusedCapacity(1);
                if (named(defaults.items, column_name, "column")) |i| _ = defaults.orderedRemove(i);
                if (replacement) |entry| defaults.appendAssumeCapacity(entry);
            }
        },
    }
    return true;
}

fn primaryKeyGrowth(allocator: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var schema = try value(a, .{ .default_type = "row", .document_schemas = .{ .row = .{ .schema = .{
        .type = "object",
        .properties = .{ .id = .{ .type = [_][]const u8{ "integer", "null" }, .nullable = true }, .tenant = .{ .type = "keyword" } },
        .additionalProperties = false,
    } } } });
    try std.testing.expect(try apply(a, &schema, .{
        .kind = .table,
        .action = .alter_schema,
        .name = .{ .table = "items" },
        .schema_change = .{ .add_unique = .{ .name = "items_pk", .columns = &.{ "tenant", "id" }, .primary = true } },
    }));
    const row = schema.object.get("document_schemas").?.object.get("row").?.object.get("schema").?;
    const required = row.object.get("required").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), required.len);
    for ([_][]const u8{ "tenant", "id" }, required) |column, entry| {
        try std.testing.expectEqualStrings(column, entry.string);
        try std.testing.expect(!row.object.get("properties").?.object.get(column).?.object.get("nullable").?.bool);
    }
    try std.testing.expectEqualStrings("integer", row.object.get("properties").?.object.get("id").?.object.get("type").?.string);
    try std.testing.expect(hasPrimary(schema));
}

fn columnNullabilityEvolution(allocator: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const compiler = @import("compiler.zig");
    var created = try compiler.compile(a, "CREATE TABLE items (id bigint PRIMARY KEY, n numeric(4,2) DEFAULT 1.25, g bigint GENERATED ALWAYS AS (id+1) STORED)", .{});
    defer created.deinit();
    var schema = try std.json.parseFromSliceLeaky(Value, a, try @import("ddl_runtime.zig").createSchemaAlloc(a, created.statement.create_table), .{});
    const defaults = try std.json.Stringify.valueAlloc(a, schema.object.get("column_defaults").?, .{});
    const generated = try std.json.Stringify.valueAlloc(a, schema.object.get("generated_columns").?, .{});
    const row = schema.object.getPtr("document_schemas").?.object.getPtr("row").?.object.getPtr("schema").?;
    try row.object.getPtr("properties").?.object.getPtr("n").?.object.put(a, "type", try value(a, [_][]const u8{ "number", "null" }));
    for ([_]struct { sql: []const u8, name: []const u8, nullable: bool }{
        .{ .sql = "ALTER TABLE items ALTER COLUMN n SET NOT NULL", .name = "n", .nullable = false },
        .{ .sql = "ALTER TABLE items ALTER n DROP NOT NULL", .name = "n", .nullable = true },
        .{ .sql = "ALTER TABLE items ALTER g SET NOT NULL", .name = "g", .nullable = false },
        .{ .sql = "ALTER TABLE items ALTER COLUMN g DROP NOT NULL", .name = "g", .nullable = true },
    }) |entry| {
        var compiled = try compiler.compile(a, entry.sql, .{});
        defer compiled.deinit();
        try std.testing.expectEqual(entry.nullable, compiled.statement.catalog_ddl.schema_change.?.set_nullability.nullable);
        try std.testing.expect(try apply(a, &schema, compiled.statement.catalog_ddl));
        const current_row = schema.object.get("document_schemas").?.object.get("row").?.object.get("schema").?;
        const property = current_row.object.get("properties").?.object.get(entry.name).?;
        try std.testing.expectEqual(entry.nullable, property.object.get("nullable").?.bool);
        var required = false;
        for (current_row.object.get("required").?.array.items) |name| if (std.mem.eql(u8, name.string, entry.name)) {
            required = true;
        };
        try std.testing.expectEqual(!entry.nullable, required);
        const before_noop = try std.json.Stringify.valueAlloc(a, schema, .{});
        try std.testing.expect(!try apply(a, &schema, compiled.statement.catalog_ddl));
        try std.testing.expectEqualStrings(before_noop, try std.json.Stringify.valueAlloc(a, schema, .{}));
        try std.testing.expectEqualStrings(defaults, try std.json.Stringify.valueAlloc(a, schema.object.get("column_defaults").?, .{}));
        try std.testing.expectEqualStrings(generated, try std.json.Stringify.valueAlloc(a, schema.object.get("generated_columns").?, .{}));
    }
    const property = schema.object.get("document_schemas").?.object.get("row").?.object.get("schema").?.object.get("properties").?.object.get("n").?;
    try std.testing.expectEqualStrings("number", property.object.get("type").?.string);
    try std.testing.expectEqualStrings("numeric", property.object.get("x-antfly-sql-type").?.string);
    try std.testing.expectEqualStrings("4", try std.json.Stringify.valueAlloc(a, property.object.get("x-antfly-sql-numeric-modifier").?.object.get("precision").?, .{}));
    for ([_]struct { sql: []const u8, err: anyerror }{
        .{ .sql = "ALTER TABLE items ALTER id DROP NOT NULL", .err = error.SqlPrimaryKeyNotNull },
        .{ .sql = "ALTER TABLE items ALTER absent SET NOT NULL", .err = error.UndefinedColumn },
    }) |entry| {
        var compiled = try compiler.compile(a, entry.sql, .{});
        defer compiled.deinit();
        const before = try std.json.Stringify.valueAlloc(a, schema, .{});
        if (apply(a, &schema, compiled.statement.catalog_ddl)) |_| return error.TestUnexpectedResult else |err| {
            if (err == error.OutOfMemory) return err;
            try std.testing.expectEqual(entry.err, err);
        }
        try std.testing.expectEqualStrings(before, try std.json.Stringify.valueAlloc(a, schema, .{}));
    }
}

test "SQL column nullability preserves expressions domains primary keys and no-op epochs" {
    try columnNullabilityEvolution(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, columnNullabilityEvolution, .{});
}

test "primary key schema lowering retains column ownership across row map growth" {
    try primaryKeyGrowth(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, primaryKeyGrowth, .{});
}

test "primary key schema lowering rejects deferred timing and empty keys" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const schema_json = "{\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}}}}}}";
    const input: ast.CatalogDdl = .{ .kind = .table, .action = .alter_schema, .name = .{ .table = "items" }, .schema_change = .{ .add_unique = .{ .name = "items_pk", .columns = &.{"id"}, .primary = true, .deferrable = true, .timing = "deferred" } } };
    var schema = try std.json.parseFromSliceLeaky(Value, alloc, schema_json, .{});
    try std.testing.expectError(error.UnsupportedSqlShape, apply(alloc, &schema, input));
    try std.testing.expect(schema.object.get("unique_constraints") == null);
    var empty = input;
    empty.schema_change = .{ .add_unique = .{ .name = "items_pk", .columns = &.{}, .primary = true } };
    try std.testing.expectError(error.InvalidSqlSyntax, apply(alloc, &schema, empty));
    try std.testing.expect(schema.object.get("unique_constraints") == null);
}

test "SQL VALIDATE UNIQUE rejects the wrong constraint kind without publishing a candidate" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var schema = try value(a, .{
        .storage_mode = "relational",
        .default_type = "row",
        .document_schemas = .{ .row = .{ .schema = .{ .type = "object", .properties = .{ .id = .{ .type = "integer" } }, .additionalProperties = false } } },
        .unique_constraints = .{.{ .name = "id_key", .columns = .{"id"} }},
    });
    const before = try std.json.Stringify.valueAlloc(a, schema, .{});
    var compiled = try @import("compiler.zig").compile(a, "ALTER TABLE items VALIDATE CONSTRAINT id_key", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlWrongObjectType, apply(a, &schema, compiled.statement.catalog_ddl));
    try std.testing.expectEqualStrings(before, try std.json.Stringify.valueAlloc(a, schema, .{}));
}
