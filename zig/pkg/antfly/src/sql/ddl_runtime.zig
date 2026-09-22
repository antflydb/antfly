// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! SQL DDL lowers into the existing native catalog/schema authority.
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");

pub const Output = struct { command_tag: []const u8, mutation_outcome: catalog.MutationOutcome, receipt: ?catalog.DdlReceipt = null };

pub fn accepts(statement: ast.Statement) bool {
    return switch (statement) {
        .create_table, .drop_table, .catalog_ddl => true,
        else => false,
    };
}

pub fn execute(alloc: std.mem.Allocator, backend: catalog.Backend, statement: ast.Statement) !Output {
    const dispatch = backend.vtable.ddl orelse return error.UnsupportedSqlExecution;
    try backend.vtable.checkpoint(backend.ptr);
    const request: catalog.Ddl = switch (statement) {
        .create_table => |create| .{ .create_table = .{ .name = create.table, .schema_json = try createSchemaAlloc(alloc, create), .if_not_exists = create.if_not_exists, .tablespace = create.tablespace } },
        .drop_table => |drop| .{ .drop_table = drop },
        .catalog_ddl => |ddl| .{ .catalog_ddl = ddl },
        else => return error.UnsupportedSqlExecution,
    };
    const outcome = try dispatch(backend.ptr, alloc, request);
    return .{ .command_tag = if (outcome.receipt != null and outcome.receipt.?.state != .ready) "DDL PENDING" else switch (request) {
        .create_table => "CREATE TABLE",
        .drop_table => "DROP TABLE",
        .catalog_ddl => |ddl| switch (ddl.action) {
            .alter_schema => switch (ddl.schema_change orelse return error.InvalidSqlSyntax) {
                .create_index => "CREATE INDEX",
                .drop_index => "DROP INDEX",
                else => "ALTER TABLE",
            },
            .create => switch (ddl.kind) {
                .database => "CREATE DATABASE",
                .namespace => "CREATE SCHEMA",
                .tablespace => "CREATE TABLESPACE",
                .table => unreachable,
            },
            .drop => switch (ddl.kind) {
                .database => "DROP DATABASE",
                .namespace => "DROP SCHEMA",
                .tablespace => "DROP TABLESPACE",
                .table => unreachable,
            },
            .rename, .set_tablespace => switch (ddl.kind) {
                .database => "ALTER DATABASE",
                .namespace => "ALTER SCHEMA",
                .tablespace => "ALTER TABLESPACE",
                .table => "ALTER TABLE",
            },
        },
    }, .mutation_outcome = outcome.mutation_outcome, .receipt = outcome.receipt };
}

/// Result belongs to the caller; temporary objects are bounded by the caller's
/// SQL statement budget. No caller-selected schema generation is introduced.
pub fn createSchemaAlloc(alloc: std.mem.Allocator, create: ast.CreateTable) anyerror![]u8 {
    if (create.columns.len == 0 or create.columns.len > 256) return error.SqlLimitExceeded;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var properties: std.json.ObjectMap = .empty;
    var required: std.ArrayList([]const u8) = .empty;
    var defaults: std.ArrayList(std.json.Value) = .empty;
    for (create.columns) |column| {
        const primary = primary: {
            for (create.constraints) |constraint| {
                if (constraint != .add_unique or !constraint.add_unique.primary) continue;
                for (constraint.add_unique.columns) |key| if (std.mem.eql(u8, key, column.name)) break :primary true;
            }
            break :primary false;
        };
        const nullable = column.nullable and !primary;
        if (std.mem.eql(u8, column.name, "_id")) return error.DuplicateSqlColumn;
        if (properties.contains(column.name)) return error.DuplicateSqlColumn;
        const property = try std.json.parseFromSliceLeaky(std.json.Value, a, try std.json.Stringify.valueAlloc(a, .{
            .type = switch (column.type) {
                .string => "keyword",
                .integer => "integer",
                .number => "number",
                .boolean => "boolean",
                .datetime => "datetime",
                .json => "json",
            },
            .nullable = nullable,
        }, .{}), .{});
        try properties.put(a, column.name, property);
        if (!nullable) try required.append(a, column.name);
        if (column.default_value) |value| {
            if (value == .parameter) return error.InvalidSqlParameters;
            const literal = try @import("describe.zig").bindLiteral(a, value, column.type);
            if (literal == .null and !nullable) return error.SqlNotNullViolation;
            try defaults.append(a, try std.json.parseFromSliceLeaky(std.json.Value, a, try std.json.Stringify.valueAlloc(a, .{ .column = column.name, .expression = .{ .op = "literal", .type = @tagName(column.type), .value = literal } }, .{}), .{ .parse_numbers = false }));
        }
    }
    const base = try std.json.Stringify.valueAlloc(a, .{
        .storage_mode = "relational",
        .default_type = "row",
        .column_defaults = defaults.items,
        .document_schemas = .{ .row = .{ .schema = .{ .type = "object", .properties = std.json.Value{ .object = properties }, .required = required.items, .additionalProperties = false } } },
    }, .{});
    var schema = try std.json.parseFromSliceLeaky(std.json.Value, a, base, .{ .parse_numbers = false });
    for (create.constraints) |constraint| {
        switch (constraint) {
            .add_unique, .add_check, .add_foreign_key => {},
            else => return error.InvalidSqlSyntax,
        }
        _ = try @import("schema_ddl.zig").apply(a, &schema, .{ .name = create.table, .kind = .table, .action = .alter_schema, .schema_change = constraint });
    }
    return std.json.Stringify.valueAlloc(alloc, schema, .{});
}

test "SQL DDL lowers exact defaults nullability and native relational types" {
    var compiled = try @import("compiler.zig").compile(std.testing.allocator, "CREATE TABLE items (id BIGINT NOT NULL, name TEXT, amount BIGINT DEFAULT 9007199254740993)", .{});
    defer compiled.deinit();
    const bytes = try createSchemaAlloc(std.testing.allocator, compiled.statement.create_table);
    defer std.testing.allocator.free(bytes);
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, bytes, .{ .parse_numbers = false });
    defer parsed.deinit();
    try std.testing.expect(parsed.value.object.get("version") == null);
    try std.testing.expectEqualStrings("9007199254740993", parsed.value.object.get("column_defaults").?.array.items[0].object.get("expression").?.object.get("value").?.number_string);
    try std.testing.expectEqualStrings("id", parsed.value.object.get("document_schemas").?.object.get("row").?.object.get("schema").?.object.get("required").?.array.items[0].string);
}

test "SQL schema DDL preserves index ownership defaults and unrelated metadata" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var create = try @import("compiler.zig").compile(alloc, "CREATE TABLE items (id BIGINT NOT NULL, title TEXT)", .{});
    defer create.deinit();
    var schema = try std.json.parseFromSliceLeaky(std.json.Value, a, try createSchemaAlloc(a, create.statement.create_table), .{ .parse_numbers = false });
    const commands = [_][]const u8{
        "CREATE UNIQUE INDEX items_id ON items (id DESC) INCLUDE (title)",
        "ALTER TABLE items ADD COLUMN enabled BOOLEAN DEFAULT TRUE",
        "ALTER TABLE items ALTER COLUMN title SET DEFAULT 'unknown'",
        "ALTER TABLE items ALTER COLUMN title DROP DEFAULT",
        "ALTER TABLE items DROP COLUMN enabled",
        "DROP INDEX items_id ON items",
    };
    for (commands, 0..) |command, i| {
        var compiled = try @import("compiler.zig").compile(alloc, command, .{});
        defer compiled.deinit();
        try std.testing.expect(try @import("schema_ddl.zig").apply(a, &schema, compiled.statement.catalog_ddl));
        if (i == 0) {
            try std.testing.expectEqual(@as(usize, 1), schema.object.get("relational_indexes").?.array.items.len);
            try std.testing.expectEqual(@as(usize, 1), schema.object.get("unique_constraints").?.array.items.len);
        }
    }
    try std.testing.expectEqual(@as(usize, 0), schema.object.get("relational_indexes").?.array.items.len);
    try std.testing.expectEqual(@as(usize, 0), schema.object.get("unique_constraints").?.array.items.len);
    try std.testing.expectEqual(@as(usize, 0), schema.object.get("column_defaults").?.array.items.len);
}

test "SQL constraints bind typed expressions and preserve composite FK actions" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var create = try @import("compiler.zig").compile(alloc, "CREATE TABLE items (id BIGINT, parent BIGINT)", .{});
    defer create.deinit();
    var schema = try std.json.parseFromSliceLeaky(std.json.Value, a, try createSchemaAlloc(a, create.statement.create_table), .{ .parse_numbers = false });
    for ([_][]const u8{
        "ALTER TABLE items ADD CONSTRAINT unique_id UNIQUE (id, parent)",
        "ALTER TABLE items ADD CONSTRAINT positive CHECK (id > 0 AND parent IS NOT NULL)",
        "ALTER TABLE items ADD CONSTRAINT fk FOREIGN KEY (parent) REFERENCES parents (id) MATCH PARTIAL ON DELETE SET NULL ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED",
    }) |sql| {
        var compiled = try @import("compiler.zig").compile(alloc, sql, .{});
        defer compiled.deinit();
        try std.testing.expect(try @import("schema_ddl.zig").apply(a, &schema, compiled.statement.catalog_ddl));
    }
    const fk = schema.object.get("foreign_keys").?.array.items[0];
    try std.testing.expectEqualStrings("partial", fk.object.get("match").?.string);
    try std.testing.expectEqualStrings("set_null", fk.object.get("on_delete").?.string);
    try std.testing.expectEqualStrings("deferred", fk.object.get("timing").?.string);
    try std.testing.expectEqualStrings("and", schema.object.get("checks").?.array.items[0].object.get("expression").?.object.get("op").?.string);
}

test "SQL CREATE TABLE combines inline primary keys and named composite declarations" {
    const alloc = std.testing.allocator;
    var compiled = try @import("compiler.zig").compile(alloc, "CREATE TABLE items (id BIGINT PRIMARY KEY, parent BIGINT REFERENCES parents (id), name TEXT CONSTRAINT unique_name UNIQUE, CONSTRAINT positive CHECK (id > 0), UNIQUE (id, name))", .{});
    defer compiled.deinit();
    const encoded = try createSchemaAlloc(alloc, compiled.statement.create_table);
    defer alloc.free(encoded);
    var schema = try std.json.parseFromSlice(std.json.Value, alloc, encoded, .{});
    defer schema.deinit();
    try std.testing.expectEqual(@as(usize, 3), schema.value.object.get("unique_constraints").?.array.items.len);
    try std.testing.expectEqual(@as(usize, 1), schema.value.object.get("foreign_keys").?.array.items.len);
    try std.testing.expectEqual(@as(usize, 1), schema.value.object.get("checks").?.array.items.len);
    try std.testing.expectEqualStrings("id", schema.value.object.get("document_schemas").?.object.get("row").?.object.get("schema").?.object.get("required").?.array.items[0].string);
}

test "SQL catalog DDL parser preserves qualified scope and native operations" {
    const compile = @import("compiler.zig").compile;
    for ([_]struct { sql: []const u8, kind: @FieldType(ast.CatalogDdl, "kind"), action: @FieldType(ast.CatalogDdl, "action") }{
        .{ .sql = "CREATE DATABASE IF NOT EXISTS analytics", .kind = .database, .action = .create },
        .{ .sql = "CREATE SCHEMA analytics.reporting", .kind = .namespace, .action = .create },
        .{ .sql = "CREATE TABLESPACE cold LOCATION 's3://bucket/path'", .kind = .tablespace, .action = .create },
        .{ .sql = "DROP SCHEMA IF EXISTS analytics.reporting", .kind = .namespace, .action = .drop },
        .{ .sql = "ALTER TABLE analytics.reporting.items RENAME TO renamed", .kind = .table, .action = .rename },
        .{ .sql = "ALTER DATABASE analytics SET TABLESPACE cold", .kind = .database, .action = .set_tablespace },
    }) |case| {
        var compiled = try compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        try std.testing.expectEqual(case.kind, compiled.statement.catalog_ddl.kind);
        try std.testing.expectEqual(case.action, compiled.statement.catalog_ddl.action);
    }
}
