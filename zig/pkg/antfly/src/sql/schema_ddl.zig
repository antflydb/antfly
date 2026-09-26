// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Pure schema changes. Caller owns the arena and commits the result with the
//! exact schema version captured from the native catalog.
const std = @import("std");
const ast = @import("ast.zig");
const Value = std.json.Value;

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
                    try keys.append(alloc, try value(alloc, .{ .expression = lowered.expression, .result_type = @tagName(lowered.type), .direction = if (key.descending) "desc" else "asc", .nulls = nulls }));
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
                const single = try @import("ddl_runtime.zig").createSchemaAlloc(alloc, .{ .table = ddl.name, .columns = &.{change.add_column} });
                const generated = try std.json.parseFromSliceLeaky(Value, alloc, single, .{ .parse_numbers = false });
                const definition = generated.object.get("document_schemas").?.object.get("row").?.object.get("schema").?;
                try properties.object.put(alloc, try alloc.dupe(u8, column_name), definition.object.get("properties").?.object.get(column_name).?);
                if (!change.add_column.nullable) {
                    const required = try list(row, alloc, "required");
                    try required.append(.{ .string = try alloc.dupe(u8, column_name) });
                }
                if (change.add_column.default_value != null) {
                    const defaults = try list(schema, alloc, "column_defaults");
                    try defaults.append(generated.object.get("column_defaults").?.array.items[0]);
                }
            } else {
                const property = properties.object.get(column_name) orelse return error.UndefinedColumn;
                if (change == .drop_column) {
                    _ = properties.object.swapRemove(column_name);
                    if (row.object.getPtr("required")) |required| {
                        for (required.array.items, 0..) |entry, i| if (entry == .string and std.mem.eql(u8, entry.string, column_name)) {
                            _ = required.array.orderedRemove(i);
                            break;
                        };
                    }
                }
                const defaults = try list(schema, alloc, "column_defaults");
                if (named(defaults.items, column_name, "column")) |i| _ = defaults.orderedRemove(i);
                if (change == .set_default) {
                    const type_name = property.object.get("type").?.string;
                    const column_type: ast.ColumnType = if (std.mem.eql(u8, type_name, "keyword")) .string else std.meta.stringToEnum(ast.ColumnType, type_name) orelse return error.UnsupportedSqlShape;
                    const literal = try @import("describe.zig").bindLiteral(alloc, change.set_default.value, column_type);
                    try defaults.append(try value(alloc, .{ .column = column_name, .expression = .{ .op = "literal", .type = @tagName(column_type), .value = literal } }));
                }
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
