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
    const properties = row.object.getPtr("properties") orelse return error.InvalidSqlBackendResponse;
    if (properties.* != .object) return error.InvalidSqlBackendResponse;
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
    const required = try list(row, alloc, "required");
    for (columns) |column| {
        const property = properties.object.getPtr(column).?;
        try property.object.put(alloc, "nullable", .{ .bool = false });
        var found = false;
        for (required.items) |entry| if (entry == .string and std.mem.eql(u8, entry.string, column)) {
            found = true;
            break;
        };
        if (!found) try required.append(.{ .string = try alloc.dupe(u8, column) });
    }
}

pub fn apply(alloc: std.mem.Allocator, schema: *Value, ddl: ast.CatalogDdl) !bool {
    var staging = std.heap.ArenaAllocator.init(alloc);
    defer staging.deinit();
    var candidate = try value(staging.allocator(), schema.*);
    const changed = try applyCandidate(staging.allocator(), &candidate, ddl);
    // Candidate allocations, including failed binding scratch, are never
    // retained by the caller. Successful result storage belongs to its schema
    // arena, as do the other JSON schema-builder operations in this module.
    if (changed) schema.* = try value(alloc, candidate);
    return changed;
}

/// In-place builder for an already unpublished schema, allowing CREATE's
/// constraint batch to avoid cloning the entire schema for every constraint.
pub fn applyCandidate(alloc: std.mem.Allocator, schema: *Value, ddl: ast.CatalogDdl) !bool {
    const change = ddl.schema_change orelse return error.InvalidSqlSyntax;
    if (schema.* != .object) return error.InvalidSqlBackendResponse;
    switch (change) {
        .drop_constraint, .validate_constraint => |constraint_name| {
            for ([_][]const u8{ "unique_constraints", "checks", "foreign_keys" }) |key| {
                const constraints = try list(schema, alloc, key);
                if (named(constraints.items, constraint_name, "name")) |position| {
                    if (change == .drop_constraint) {
                        _ = constraints.orderedRemove(position);
                        if (std.mem.eql(u8, key, "unique_constraints")) {
                            const indexes = try list(schema, alloc, "relational_indexes");
                            if (named(indexes.items, constraint_name, "name")) |index| {
                                const description = indexes.items[index].object.get("description");
                                if (description) |text| if (text == .string and std.mem.eql(u8, text.string, "SQL UNIQUE INDEX")) {
                                    _ = indexes.orderedRemove(index);
                                };
                            }
                        }
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
            try constraints.append(try value(alloc, .{ .name = constraint.name, .columns = constraint.columns, .primary = constraint.primary, .deferrable = constraint.deferrable, .timing = constraint.timing }));
        },
        .add_check => |constraint| {
            if (constraintExists(schema.*, constraint.name)) return error.SqlConstraintAlreadyExists;
            const expression = try @import("schema_expression.zig").lower(alloc, schema.*, constraint.expression, .boolean);
            const constraints = try list(schema, alloc, "checks");
            if (named(constraints.items, constraint.name, "name") != null) return error.SqlConstraintAlreadyExists;
            try constraints.append(try value(alloc, .{ .name = constraint.name, .expression = expression }));
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
                    const lowered = try @import("schema_expression.zig").lowerTyped(alloc, schema.*, expression, null);
                    try keys.append(alloc, try value(alloc, .{ .expression = lowered.expression, .result_type = if (lowered.type == .uuid) "string" else @tagName(lowered.type), .direction = if (key.descending) "desc" else "asc", .nulls = nulls }));
                } else try keys.append(alloc, try value(alloc, .{ .column = key.field, .direction = if (key.descending) "desc" else "asc", .nulls = nulls }));
                try columns.append(alloc, key.field);
            }
            // The description marks ownership of the paired uniqueness rule;
            // native catalog validation and the entire update share one CAS.
            const predicates = if (index.predicate) |predicate| try @import("schema_expression.zig").lowerIndexPredicate(alloc, schema.*, predicate) else &.{};
            try indexes.append(try value(alloc, .{ .name = index.name, .keys = keys.items, .include_columns = index.include_columns, .where = predicates, .description = if (index.unique) "SQL UNIQUE INDEX" else "SQL INDEX" }));
            if (index.unique) {
                const constraints = try list(schema, alloc, "unique_constraints");
                if (named(constraints.items, index.name, "name") != null) return error.SqlConstraintAlreadyExists;
                const has_expression = for (index.keys) |key| {
                    if (key.expression != null) break true;
                } else false;
                try constraints.append(if (has_expression)
                    try value(alloc, .{ .name = index.name, .keys = keys.items, .where = predicates })
                else
                    try value(alloc, .{ .name = index.name, .columns = columns.items, .where = predicates }));
            }
        },
        .drop_index => |index_name| {
            const indexes = try list(schema, alloc, "relational_indexes");
            const position = named(indexes.items, index_name, "name") orelse {
                if (ddl.conditional) return false;
                return error.SqlIndexNotFound;
            };
            const description = indexes.items[position].object.get("description");
            const owned_unique = if (description) |d| d == .string and std.mem.eql(u8, d.string, "SQL UNIQUE INDEX") else false;
            _ = indexes.orderedRemove(position);
            if (owned_unique) {
                const constraints = try list(schema, alloc, "unique_constraints");
                const unique_position = named(constraints.items, index_name, "name") orelse return error.InvalidSqlBackendResponse;
                _ = constraints.orderedRemove(unique_position);
            }
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
                else => unreachable,
            };
            if (std.mem.eql(u8, column_name, "_id")) return error.UnsupportedSqlShape;
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
                    const lowered = try @import("schema_expression.zig").lowerAssignment(alloc, schema.*, expression, column, false);
                    try defaults.append(try value(alloc, .{ .column = column_name, .expression = lowered }));
                }
                if (column.generated_expression) |expression| {
                    const generated = try list(schema, alloc, "generated_columns");
                    const lowered = try @import("schema_expression.zig").lowerAssignment(alloc, schema.*, expression, column, true);
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
                    const expression = try @import("schema_expression.zig").lowerAssignment(alloc, schema.*, change.set_default.expression, .{ .name = column_name, .type = column_type, .element_type = element }, false);
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
