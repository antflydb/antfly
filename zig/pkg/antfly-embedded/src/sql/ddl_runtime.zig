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

//! SQL DDL lowers into the existing native catalog/schema authority.
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");

pub const Output = struct { command_tag: []const u8, mutation_outcome: ?catalog.MutationOutcome, receipt: ?catalog.DdlReceipt = null };

pub fn accepts(statement: ast.Statement) bool {
    return switch (statement) {
        .create_table, .drop_table, .catalog_ddl, .policy_ddl => true,
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
        .policy_ddl => |ddl| .{ .policy_ddl = ddl },
        else => return error.UnsupportedSqlExecution,
    };
    const outcome = try dispatch(backend.ptr, alloc, request);
    return .{ .command_tag = if (outcome.mutation_outcome == .committed_pending or (outcome.receipt != null and outcome.receipt.?.state != .ready)) "DDL PENDING" else switch (request) {
        .create_table => "CREATE TABLE",
        .drop_table => "DROP TABLE",
        .policy_ddl => |ddl| switch (ddl.action) {
            .create => "CREATE POLICY",
            .alter => "ALTER POLICY",
            .drop => "DROP POLICY",
            .enable => "ALTER TABLE ENABLE ROW LEVEL SECURITY",
            .disable => "ALTER TABLE DISABLE ROW LEVEL SECURITY",
        },
        .catalog_ddl => |ddl| switch (ddl.action) {
            .truncate => "TRUNCATE TABLE",
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
    var generated: std.ArrayList(std.json.Value) = .empty;
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
        try properties.put(a, column.name, try columnProperty(a, column, nullable));
        if (!nullable) try required.append(a, column.name);
        if (column.generated_expression != null) {
            if (column.default_expression != null) return error.InvalidSqlSyntax;
            try generated.append(a, try std.json.parseFromSliceLeaky(std.json.Value, a, try std.json.Stringify.valueAlloc(a, .{ .column = column.name, .expression = @as(?u8, null) }, .{}), .{}));
        }
    }
    const base = try std.json.Stringify.valueAlloc(a, .{
        .storage_mode = "relational",
        .default_type = "row",
        .column_defaults = @as([]const std.json.Value, &.{}),
        .generated_columns = generated.items,
        .document_schemas = .{ .row = .{ .schema = .{ .type = "object", .properties = std.json.Value{ .object = properties }, .required = required.items, .additionalProperties = false } } },
    }, .{});
    var schema = try std.json.parseFromSliceLeaky(std.json.Value, a, base, .{ .parse_numbers = false });
    // Bind only after every declared base/generated name is present. Forward
    // base references work; any generated-to-generated reference is rejected.
    var generated_index: usize = 0;
    for (create.columns) |column| {
        if (column.default_expression) |expression| {
            const lowered = try @import("schema_expression.zig").lowerAssignment(a, schema, expression, column, false);
            try schema.object.getPtr("column_defaults").?.array.append(try std.json.parseFromSliceLeaky(std.json.Value, a, try std.json.Stringify.valueAlloc(a, .{ .column = column.name, .expression = lowered }, .{}), .{ .parse_numbers = false }));
        }
        if (column.generated_expression) |expression| {
            const lowered = try @import("schema_expression.zig").lowerAssignment(a, schema, expression, column, true);
            try schema.object.getPtr("generated_columns").?.array.items[generated_index].object.put(a, "expression", lowered);
            generated_index += 1;
        }
    }
    for (create.constraints) |constraint| {
        switch (constraint) {
            .add_unique, .add_check, .add_foreign_key => {},
            else => return error.InvalidSqlSyntax,
        }
        _ = try @import("schema_ddl.zig").applyCandidate(a, &schema, .{ .name = create.table, .kind = .table, .action = .alter_schema, .schema_change = constraint });
    }
    return std.json.Stringify.valueAlloc(alloc, schema, .{});
}

pub fn columnProperty(alloc: std.mem.Allocator, column: ast.Column, nullable: bool) !std.json.Value {
    const bytes = if (column.type == .uuid)
        try std.json.Stringify.valueAlloc(alloc, .{ .type = "keyword", .nullable = nullable, .format = "uuid" }, .{})
    else
        try std.json.Stringify.valueAlloc(alloc, .{
            .type = switch (column.type) {
                .array => return error.UnsupportedSqlShape,
                .string => "keyword",
                .uuid => unreachable,
                .integer => "integer",
                .number => "number",
                .boolean => "boolean",
                .datetime => "datetime",
                .json => "json",
            },
            .nullable = nullable,
        }, .{});
    var property = try std.json.parseFromSliceLeaky(std.json.Value, alloc, bytes, .{});
    if (column.element_type) |kind| try property.object.put(alloc, "x-antfly-sql-type", .{ .string = @tagName(kind) });
    return property;
}

pub fn bindDefault(alloc: std.mem.Allocator, value: ast.Value, kind: ast.ColumnType, element: ?@import("array_value.zig").ElementType) !std.json.Value {
    const describe = @import("describe.zig");
    const literal = try describe.bindLiteral(alloc, value, kind);
    return (try describe.coerceDatum(alloc, .{ .value = literal, .sql_null = literal == .null }, kind, element)).value;
}

/// PostgreSQL retains assignment casts on numeric defaults. Their overflow is
/// an INSERT/UPDATE DEFAULT failure, not a schema-publication failure. Keep
/// the source literal separate from its target domain in the durable plan.
pub fn defaultExpression(alloc: std.mem.Allocator, value: ast.Value, kind: ast.ColumnType, element: ?@import("array_value.zig").ElementType) !std.json.Value {
    if (value == .numeric and kind == .number) {
        if (element == .numeric) return error.UnsupportedSqlShape;
        // Native real/double defaults retain their assignment cast. Parse the
        // exact literal directly at the requested width, avoiding a double
        // rounding through f64 for real defaults.
        const exact = @import("numeric_value.zig");
        var context: exact.Context = .{ .alloc = alloc };
        var source = try exact.parse(&context, value.numeric);
        defer source.deinit();
        const text = try exact.format(&context, source.value);
        defer alloc.free(text);
        const casts = @import("builtin_cast.zig");
        const number = if (element == .float32) try casts.floatValue(f32, .{ .string = text }) else try casts.floatValue(f64, .{ .string = text });
        return defaultExpression(alloc, .{ .number = number }, kind, element);
    }
    const numeric = (kind == .integer and value == .integer) or (kind == .number and (value == .integer or value == .number));
    if (numeric) {
        const source: ast.ColumnType = if (value == .integer) .integer else .number;
        const source_type: @import("array_value.zig").ElementType = if (value == .integer) (if (std.math.cast(i32, value.integer) != null) .int32 else .int64) else .float64;
        const literal: std.json.Value = if (value == .integer) .{ .integer = value.integer } else .{ .float = value.number };
        const target_type: @import("array_value.zig").ElementType = element orelse if (kind == .integer) .int64 else .float64;
        return std.json.parseFromSliceLeaky(std.json.Value, alloc, try std.json.Stringify.valueAlloc(alloc, .{
            .op = "cast",
            .type = @tagName(kind),
            .sql_type = @tagName(target_type),
            .args = &.{.{ .op = "literal", .type = @tagName(source), .sql_type = @tagName(source_type), .value = literal }},
        }, .{}), .{ .parse_numbers = false });
    }
    const literal = try bindDefault(alloc, value, kind, element);
    return std.json.parseFromSliceLeaky(std.json.Value, alloc, try std.json.Stringify.valueAlloc(alloc, .{ .op = "literal", .type = if (kind == .uuid) "string" else @tagName(kind), .value = literal }, .{}), .{ .parse_numbers = false });
}

test "SQL TRUNCATE lowers complete table set and honest durable admission receipt" {
    const Fake = struct {
        fn ddl(_: *anyopaque, _: std.mem.Allocator, request: catalog.Ddl) !catalog.DdlOutcome {
            const input = request.catalog_ddl;
            try std.testing.expectEqual(@as(usize, 2), input.truncate_tables.len);
            try std.testing.expect(input.cascade and input.restart_identity);
            try std.testing.expectEqualStrings("first", input.truncate_tables[0].table);
            return .{ .mutation_outcome = null, .receipt = .{ .database = "default", .namespace = "public", .table = "first", .table_id = "1", .schema_version = 1, .state = .admission_unknown, .restore_job_id = "42" } };
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    var sentinel: u8 = 0;
    var compiled = try @import("compiler.zig").compile(std.testing.allocator, "TRUNCATE TABLE first, second RESTART IDENTITY CASCADE", .{});
    defer compiled.deinit();
    const result = try execute(std.testing.allocator, .{ .ptr = &sentinel, .vtable = &.{ .resolve = undefined, .scan = undefined, .mutate = undefined, .ddl = Fake.ddl, .checkpoint = Fake.checkpoint } }, compiled.statement);
    try std.testing.expectEqualStrings("DDL PENDING", result.command_tag);
    try std.testing.expect(result.mutation_outcome == null);
    try std.testing.expectEqualStrings("42", result.receipt.?.restore_job_id.?);
    var continued = try @import("compiler.zig").compile(std.testing.allocator, "TRUNCATE first CONTINUE IDENTITY RESTRICT", .{});
    defer continued.deinit();
    try std.testing.expect(!continued.statement.catalog_ddl.restart_identity and !continued.statement.catalog_ddl.cascade);
}

test "SQL DDL lowers exact defaults nullability and native relational types" {
    var compiled = try @import("compiler.zig").compile(std.testing.allocator, "CREATE TABLE items (id BIGINT NOT NULL, name TEXT, amount BIGINT DEFAULT 9007199254740993)", .{});
    defer compiled.deinit();
    const bytes = try createSchemaAlloc(std.testing.allocator, compiled.statement.create_table);
    defer std.testing.allocator.free(bytes);
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, bytes, .{ .parse_numbers = false });
    defer parsed.deinit();
    try std.testing.expect(parsed.value.object.get("version") == null);
    try std.testing.expectEqualStrings("9007199254740993", parsed.value.object.get("column_defaults").?.array.items[0].object.get("expression").?.object.get("args").?.array.items[0].object.get("value").?.number_string);
    try std.testing.expectEqualStrings("id", parsed.value.object.get("document_schemas").?.object.get("row").?.object.get("schema").?.object.get("required").?.array.items[0].string);
}

test "SQL precise scalar DDL preserves CREATE ALTER default assignment casts" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var compiled = try @import("compiler.zig").compile(a, "CREATE TABLE widths (n smallint DEFAULT 32767, f real DEFAULT 0.1, b bigint DEFAULT 9007199254740993)", .{});
    defer compiled.deinit();
    var schema = try std.json.parseFromSliceLeaky(std.json.Value, a, try createSchemaAlloc(a, compiled.statement.create_table), .{});
    const properties = schema.object.get("document_schemas").?.object.get("row").?.object.get("schema").?.object.get("properties").?.object;
    try std.testing.expectEqualStrings("int16", properties.get("n").?.object.get("x-antfly-sql-type").?.string);
    try std.testing.expectEqualStrings("float32", properties.get("f").?.object.get("x-antfly-sql-type").?.string);
    const defaults = schema.object.get("column_defaults").?.array.items;
    try std.testing.expectEqualStrings("float32", defaults[1].object.get("expression").?.object.get("sql_type").?.string);
    try std.testing.expectEqual(@as(f64, @as(f32, 0.1)), defaults[1].object.get("expression").?.object.get("args").?.array.items[0].object.get("value").?.float);
    for ([_][]const u8{ "CREATE TABLE bad (n smallint DEFAULT 32768)", "CREATE TABLE bad (n integer DEFAULT 2147483648)" }) |sql| {
        var bad = try @import("compiler.zig").compile(a, sql, .{});
        defer bad.deinit();
        _ = try createSchemaAlloc(a, bad.statement.create_table);
    }
    var alter = try @import("compiler.zig").compile(a, "ALTER TABLE widths ALTER COLUMN n SET DEFAULT 32768", .{});
    defer alter.deinit();
    try std.testing.expect(try @import("schema_ddl.zig").apply(a, &schema, alter.statement.catalog_ddl));
    try std.testing.expectEqualStrings("int16", schema.object.get("column_defaults").?.array.items[2].object.get("expression").?.object.get("sql_type").?.string);
}

test "SQL ALTER DEFAULT uses explicit builtin identity with nullable union schemas" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    var schema = try std.json.parseFromSliceLeaky(std.json.Value, alloc,
        \\{"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"u":{"type":["string","null"],"x-antfly-sql-type":"uuid"},"n":{"type":["integer","null"],"x-antfly-sql-type":"int16"}},"additionalProperties":false}}}}
    , .{});
    var valid = try @import("compiler.zig").compile(alloc, "ALTER TABLE widths ALTER COLUMN u SET DEFAULT '{A0EEBC999C0B4EF8BB6D6BB9BD380A11}'", .{});
    defer valid.deinit();
    try std.testing.expect(try @import("schema_ddl.zig").apply(alloc, &schema, valid.statement.catalog_ddl));
    try std.testing.expectEqualStrings("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", schema.object.get("column_defaults").?.array.items[0].object.get("expression").?.object.get("value").?.string);
    var numeric = try @import("compiler.zig").compile(alloc, "ALTER TABLE widths ALTER COLUMN n SET DEFAULT 32768", .{});
    defer numeric.deinit();
    try std.testing.expect(try @import("schema_ddl.zig").apply(alloc, &schema, numeric.statement.catalog_ddl));
    const assignment = schema.object.get("column_defaults").?.array.items[1].object.get("expression").?;
    try std.testing.expectEqualStrings("cast", assignment.object.get("op").?.string);
    try std.testing.expectEqualStrings("int16", assignment.object.get("sql_type").?.string);
    try std.testing.expectEqualStrings("32768", assignment.object.get("args").?.array.items[0].object.get("value").?.number_string);
}

test "SQL expression DDL binds defaults and generated columns against the complete candidate" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var created = try @import("compiler.zig").compile(a, "CREATE TABLE exprs (g integer GENERATED ALWAYS AS (CASE WHEN n IS NULL THEN 0 ELSE CAST(n AS integer)+1 END) STORED, n smallint DEFAULT (32767+1), label text DEFAULT lower('READY'), slug text GENERATED ALWAYS AS (lower(label)||'-ok') STORED)", .{});
    defer created.deinit();
    const bytes = try createSchemaAlloc(a, created.statement.create_table);
    var schema = try std.json.parseFromSliceLeaky(std.json.Value, a, bytes, .{ .parse_numbers = false });
    try std.testing.expectEqual(@as(usize, 2), schema.object.get("generated_columns").?.array.items.len);
    try std.testing.expectEqualStrings("int16", schema.object.get("column_defaults").?.array.items[0].object.get("expression").?.object.get("sql_type").?.string);
    for ([_][]const u8{
        "ALTER TABLE exprs ALTER COLUMN n SET DEFAULT (2+3)",
        "ALTER TABLE exprs ADD COLUMN h bigint GENERATED ALWAYS AS (n*2) STORED NOT NULL",
        "ALTER TABLE exprs ADD COLUMN extra integer DEFAULT (4*5) NOT NULL",
    }) |sql| {
        var compiled = try @import("compiler.zig").compile(a, sql, .{});
        defer compiled.deinit();
        try std.testing.expect(try @import("schema_ddl.zig").apply(a, &schema, compiled.statement.catalog_ddl));
    }
    try std.testing.expectEqual(@as(usize, 3), schema.object.get("generated_columns").?.array.items.len);
    const before = try std.json.Stringify.valueAlloc(a, schema, .{});
    for ([_]struct { sql: []const u8, failure: anyerror }{
        .{ .sql = "ALTER TABLE exprs ADD COLUMN bad integer GENERATED ALWAYS AS (g+1) STORED", .failure = error.SqlInvalidGenerationExpression },
        .{ .sql = "ALTER TABLE exprs ADD COLUMN bad integer GENERATED ALWAYS AS (absent+1) STORED", .failure = error.UndefinedColumn },
        .{ .sql = "ALTER TABLE exprs ALTER COLUMN g SET DEFAULT 5", .failure = error.InvalidSqlSyntax },
        .{ .sql = "ALTER TABLE exprs ADD COLUMN bad real DEFAULT CAST(0.1+0.2 AS double precision)", .failure = error.UnsupportedSqlShape },
    }) |case| {
        var compiled = try @import("compiler.zig").compile(a, case.sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(case.failure, @import("schema_ddl.zig").apply(a, &schema, compiled.statement.catalog_ddl));
        try std.testing.expectEqualStrings(before, try std.json.Stringify.valueAlloc(a, schema, .{}));
    }
    var dropped = try @import("compiler.zig").compile(a, "ALTER TABLE exprs DROP COLUMN h", .{});
    defer dropped.deinit();
    try std.testing.expect(try @import("schema_ddl.zig").apply(a, &schema, dropped.statement.catalog_ddl));
    try std.testing.expectEqual(@as(usize, 2), schema.object.get("generated_columns").?.array.items.len);
}

test "SQL expression DDL staging unwinds every allocation fault" {
    const Fixture = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const a = arena.allocator();
            var created = try @import("compiler.zig").compile(a, "CREATE TABLE exprs (n smallint DEFAULT (2+3), g integer GENERATED ALWAYS AS (n+1) STORED)", .{});
            defer created.deinit();
            const bytes = try createSchemaAlloc(a, created.statement.create_table);
            var schema = try std.json.parseFromSliceLeaky(std.json.Value, a, bytes, .{});
            var added = try @import("compiler.zig").compile(a, "ALTER TABLE exprs ADD COLUMN h integer GENERATED ALWAYS AS (n+2) STORED", .{});
            defer added.deinit();
            _ = try @import("schema_ddl.zig").apply(a, &schema, added.statement.catalog_ddl);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL array DDL refuses schema publication before typed storage admission" {
    var create = try @import("compiler.zig").compile(std.testing.allocator, "CREATE TABLE arrays (a int4[2][3])", .{});
    defer create.deinit();
    try std.testing.expectError(error.UnsupportedSqlShape, createSchemaAlloc(std.testing.allocator, create.statement.create_table));
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    var base = try @import("compiler.zig").compile(alloc, "CREATE TABLE arrays (id bigint)", .{});
    defer base.deinit();
    const original = try createSchemaAlloc(alloc, base.statement.create_table);
    var schema = try std.json.parseFromSliceLeaky(std.json.Value, alloc, original, .{});
    var alter = try @import("compiler.zig").compile(alloc, "ALTER TABLE arrays ADD COLUMN a jsonb[]", .{});
    defer alter.deinit();
    try std.testing.expectError(error.UnsupportedSqlShape, @import("schema_ddl.zig").apply(alloc, &schema, alter.statement.catalog_ddl));
    try std.testing.expectEqualStrings(original, try std.json.Stringify.valueAlloc(alloc, schema, .{}));
}

test "SQL UUID CREATE TABLE retains typed native schema format" {
    var compiled = try @import("compiler.zig").compile(std.testing.allocator, "CREATE TABLE prepared_usage_records (id uuid)", .{});
    defer compiled.deinit();
    const bytes = try createSchemaAlloc(std.testing.allocator, compiled.statement.create_table);
    defer std.testing.allocator.free(bytes);
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, bytes, .{});
    defer parsed.deinit();
    const property = parsed.value.object.get("document_schemas").?.object.get("row").?.object.get("schema").?.object.get("properties").?.object.get("id").?;
    try std.testing.expectEqualStrings("keyword", property.object.get("type").?.string);
    try std.testing.expectEqualStrings("uuid", property.object.get("format").?.string);
}

test "SQL UUID default and index DDL retain canonical value and native string key" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    var create = try @import("compiler.zig").compile(alloc, "CREATE TABLE items (id uuid)", .{});
    defer create.deinit();
    var schema = try std.json.parseFromSliceLeaky(std.json.Value, alloc, try createSchemaAlloc(alloc, create.statement.create_table), .{});
    var default = try @import("compiler.zig").compile(alloc, "ALTER TABLE items ALTER COLUMN id SET DEFAULT '{A0EEBC999C0B4EF8BB6D6BB9BD380A11}'", .{});
    defer default.deinit();
    try std.testing.expect(try @import("schema_ddl.zig").apply(alloc, &schema, default.statement.catalog_ddl));
    const expression = schema.object.get("column_defaults").?.array.items[0].object.get("expression").?;
    try std.testing.expectEqualStrings("string", expression.object.get("type").?.string);
    try std.testing.expectEqualStrings("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", expression.object.get("value").?.string);
    var index = try @import("compiler.zig").compile(alloc, "CREATE INDEX items_id ON items (id)", .{});
    defer index.deinit();
    try std.testing.expect(try @import("schema_ddl.zig").apply(alloc, &schema, index.statement.catalog_ddl));
    const key = schema.object.get("relational_indexes").?.array.items[0].object.get("keys").?.array.items[0];
    try std.testing.expectEqualStrings("id", key.object.get("column").?.string);
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
        "CREATE UNIQUE INDEX items_lower_title ON items ((lower(title))) WHERE title IS NOT NULL",
        "DROP INDEX items_lower_title ON items",
    };
    for (commands, 0..) |command, i| {
        var compiled = try @import("compiler.zig").compile(alloc, command, .{});
        defer compiled.deinit();
        try std.testing.expect(try @import("schema_ddl.zig").apply(a, &schema, compiled.statement.catalog_ddl));
        if (i == 0) {
            try std.testing.expectEqual(@as(usize, 1), schema.object.get("relational_indexes").?.array.items.len);
            try std.testing.expectEqual(@as(usize, 1), schema.object.get("unique_constraints").?.array.items.len);
        }
        if (i == 6) {
            const constraint = schema.object.get("unique_constraints").?.array.items[0];
            try std.testing.expect(constraint.object.get("columns") == null);
            try std.testing.expectEqual(@as(usize, 1), constraint.object.get("keys").?.array.items.len);
            try std.testing.expectEqual(@as(usize, 1), constraint.object.get("where").?.array.items.len);
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
    try std.testing.expect(schema.value.object.get("unique_constraints").?.array.items[0].object.get("primary").?.bool);
    try std.testing.expectEqual(@as(usize, 1), schema.value.object.get("foreign_keys").?.array.items.len);
    try std.testing.expectEqual(@as(usize, 1), schema.value.object.get("checks").?.array.items.len);
    try std.testing.expectEqualStrings("id", schema.value.object.get("document_schemas").?.object.get("row").?.object.get("schema").?.object.get("required").?.array.items[0].string);
    var second = try @import("compiler.zig").compile(alloc, "ALTER TABLE items ADD CONSTRAINT another_pk PRIMARY KEY (name)", .{});
    defer second.deinit();
    try std.testing.expectError(error.SqlConstraintAlreadyExists, @import("schema_ddl.zig").apply(alloc, &schema.value, second.statement.catalog_ddl));
}

test "SQL ALTER TABLE primary key marks every key column nonnullable with one unique declaration" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const schema_alloc = arena.allocator();
    var create = try @import("compiler.zig").compile(alloc, "CREATE TABLE items (tenant BIGINT, id BIGINT, note TEXT)", .{});
    defer create.deinit();
    const encoded = try createSchemaAlloc(alloc, create.statement.create_table);
    defer alloc.free(encoded);
    var schema = try std.json.parseFromSliceLeaky(std.json.Value, schema_alloc, encoded, .{ .parse_numbers = false });
    var invalid = try @import("compiler.zig").compile(alloc, "ALTER TABLE items ADD CONSTRAINT bad_pk PRIMARY KEY (tenant, missing)", .{});
    defer invalid.deinit();
    try std.testing.expectError(error.UndefinedColumn, @import("schema_ddl.zig").apply(schema_alloc, &schema, invalid.statement.catalog_ddl));
    try std.testing.expectEqual(true, schema.object.get("document_schemas").?.object.get("row").?.object.get("schema").?.object.get("properties").?.object.get("tenant").?.object.get("nullable").?.bool);
    try std.testing.expect(schema.object.get("unique_constraints") == null);
    var alter = try @import("compiler.zig").compile(alloc, "ALTER TABLE items ADD CONSTRAINT items_pk PRIMARY KEY (tenant, id)", .{});
    defer alter.deinit();
    try std.testing.expect(try @import("schema_ddl.zig").apply(schema_alloc, &schema, alter.statement.catalog_ddl));
    const row = schema.object.get("document_schemas").?.object.get("row").?.object.get("schema").?;
    const properties = row.object.get("properties").?.object;
    try std.testing.expectEqual(false, properties.get("tenant").?.object.get("nullable").?.bool);
    try std.testing.expectEqual(false, properties.get("id").?.object.get("nullable").?.bool);
    try std.testing.expectEqual(true, properties.get("note").?.object.get("nullable").?.bool);
    const required = row.object.get("required").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), required.len);
    try std.testing.expectEqualStrings("tenant", required[0].string);
    try std.testing.expectEqualStrings("id", required[1].string);
    const uniques = schema.object.get("unique_constraints").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), uniques.len);
    try std.testing.expectEqualStrings("items_pk", uniques[0].object.get("name").?.string);
    try std.testing.expect(uniques[0].object.get("primary").?.bool);
    var second = try @import("compiler.zig").compile(alloc, "ALTER TABLE items ADD CONSTRAINT other_pk PRIMARY KEY (note)", .{});
    defer second.deinit();
    try std.testing.expectError(error.SqlConstraintAlreadyExists, @import("schema_ddl.zig").apply(schema_alloc, &schema, second.statement.catalog_ddl));
    const final_json = try std.json.Stringify.valueAlloc(alloc, schema, .{});
    defer alloc.free(final_json);
    var validated = try @import("../schema/mod.zig").parseValidatedTableSchema(alloc, final_json);
    defer validated.deinit(alloc);
    const nullable_primary = try std.mem.replaceOwned(u8, alloc, final_json, "\"nullable\":false", "\"nullable\":true");
    defer alloc.free(nullable_primary);
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, @import("../schema/mod.zig").parseValidatedTableSchema(alloc, nullable_primary));
    const optional_primary = try std.mem.replaceOwned(u8, alloc, final_json, "\"required\":[\"tenant\",\"id\"]", "\"required\":[]");
    defer alloc.free(optional_primary);
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, @import("../schema/mod.zig").parseValidatedTableSchema(alloc, optional_primary));
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
