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

fn generatedConstraintNamesAlloc(a: std.mem.Allocator, create: ast.CreateTable) ![]const []const u8 {
    var count: usize = 0;
    for (create.constraints) |constraint| if (constraint.generatedConstraintName() != null) {
        count += 1;
    };
    if (count == 0) return &.{};
    const result = try a.alloc([]const u8, count);
    var index: usize = 0;
    for (create.constraints) |constraint| if (constraint.generatedConstraintName()) |name| {
        result[index] = name;
        index += 1;
    };
    return result;
}

pub fn execute(allocators: catalog.DdlAllocators, backend: catalog.Backend, statement: ast.Statement) !Output {
    const alloc = allocators.scratch;
    const dispatch = backend.vtable.ddl orelse return error.UnsupportedSqlExecution;
    try backend.vtable.checkpoint(backend.ptr);
    const preparation: @import("immutable_plan.zig").Limits = if (backend.scalar_control) |control| .{ .checkpoint = control.checkpoint, .checkpoint_context = control.ptr } else .{};
    const generated_names = if (statement == .create_table) try generatedConstraintNamesAlloc(alloc, statement.create_table) else &.{};
    defer alloc.free(generated_names);
    const request: catalog.Ddl = switch (statement) {
        .create_table => |create| .{ .create_table = .{ .name = create.table, .schema_json = try createSchemaAllocWithLimits(alloc, create, preparation), .if_not_exists = create.if_not_exists, .tablespace = create.tablespace, .generated_constraint_names = generated_names } },
        .drop_table => |drop| .{ .drop_table = drop },
        .catalog_ddl => |ddl| .{ .catalog_ddl = ddl },
        .policy_ddl => |ddl| .{ .policy_ddl = ddl },
        else => return error.UnsupportedSqlExecution,
    };

    defer if (request == .create_table) {
        alloc.free(request.create_table.schema_json);
    };
    try backend.vtable.checkpoint(backend.ptr);
    const outcome = try dispatch(backend.ptr, allocators, request);
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
                .index => "CREATE INDEX",
                .view => "CREATE VIEW",
                .table => unreachable,
            },
            .drop => switch (ddl.kind) {
                .database => "DROP DATABASE",
                .namespace => "DROP SCHEMA",
                .tablespace => "DROP TABLESPACE",
                .index => "DROP INDEX",
                .view => "DROP VIEW",
                .table => unreachable,
            },
            .rename, .set_tablespace => switch (ddl.kind) {
                .database => "ALTER DATABASE",
                .namespace => "ALTER SCHEMA",
                .tablespace => "ALTER TABLESPACE",
                .table => "ALTER TABLE",
                .index => "ALTER INDEX",
                .view => "ALTER VIEW",
            },
        },
    }, .mutation_outcome = outcome.mutation_outcome, .receipt = outcome.receipt };
}

/// Result belongs to the caller; temporary objects are bounded by the caller's
/// SQL statement budget. No caller-selected schema generation is introduced.
pub fn createSchemaAlloc(alloc: std.mem.Allocator, create: ast.CreateTable) anyerror![]u8 {
    return createSchemaAllocWithLimits(alloc, create, .{});
}

pub fn createSchemaAllocWithLimits(alloc: std.mem.Allocator, create: ast.CreateTable, limits: @import("immutable_plan.zig").Limits) anyerror![]u8 {
    if (limits.checkpoint) |check| try check(limits.checkpoint_context);
    if (create.columns.len == 0 or create.columns.len > 256) return error.SqlLimitExceeded;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var properties: std.json.ObjectMap = .empty;
    var required: std.ArrayList([]const u8) = .empty;
    var generated: std.ArrayList(std.json.Value) = .empty;
    for (create.columns) |column| {
        if (limits.checkpoint) |check| try check(limits.checkpoint_context);
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
        if (limits.checkpoint) |check| try check(limits.checkpoint_context);
        if (column.default_expression) |expression| {
            const lowered = try @import("schema_expression.zig").lowerAssignmentWithLimits(a, schema, expression, column, false, limits);
            try schema.object.getPtr("column_defaults").?.array.append(try std.json.parseFromSliceLeaky(std.json.Value, a, try std.json.Stringify.valueAlloc(a, .{ .column = column.name, .expression = lowered }, .{}), .{ .parse_numbers = false }));
        }
        if (column.generated_expression) |expression| {
            const lowered = try @import("schema_expression.zig").lowerAssignmentWithLimits(a, schema, expression, column, true, limits);
            try schema.object.getPtr("generated_columns").?.array.items[generated_index].object.put(a, "expression", lowered);
            generated_index += 1;
        }
    }
    for (create.constraints) |constraint| {
        switch (constraint) {
            .add_unique, .add_check, .add_foreign_key => {},
            else => return error.InvalidSqlSyntax,
        }
        _ = try @import("schema_ddl.zig").applyCandidateWithLimits(a, &schema, .{ .name = create.table, .kind = .table, .action = .alter_schema, .schema_change = constraint }, limits);
    }
    return std.json.Stringify.valueAlloc(alloc, schema, .{});
}

test "SQL NUMERIC modifier declarations publish exact constrained column identity" {
    var compiled = try @import("compiler.zig").compile(std.testing.allocator, "CREATE TABLE constrained (n numeric(4,2))", .{});
    defer compiled.deinit();
    const column = compiled.statement.create_table.columns[0];
    try std.testing.expectEqual(@as(u16, 4), column.numeric_modifier.?.precision);
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const property = try columnProperty(arena.allocator(), column, true);
    const resolved = try @import("schema_columns.zig").column("n", property);
    try std.testing.expectEqual(column.numeric_modifier, resolved.numeric_modifier);
}

pub fn columnProperty(alloc: std.mem.Allocator, column: ast.Column, nullable: bool) !std.json.Value {
    if (column.numeric_modifier != null and ((column.type != .number and column.type != .array) or column.element_type != .numeric)) return error.UnsupportedSqlShape;
    if (column.type == .array and column.element_type == null) return error.UnsupportedSqlShape;
    const bytes = if (column.type == .uuid)
        try std.json.Stringify.valueAlloc(alloc, .{ .type = "keyword", .nullable = nullable, .format = "uuid" }, .{})
    else
        try std.json.Stringify.valueAlloc(alloc, .{
            .type = switch (column.type) {
                .array => "sql_array",
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
    if (column.numeric_modifier) |modifier| {
        try modifier.validate();
        const value = try std.json.parseFromSliceLeaky(std.json.Value, alloc, try std.json.Stringify.valueAlloc(alloc, modifier, .{}), .{});
        try property.object.put(alloc, "x-antfly-sql-numeric-modifier", value);
    }
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
    if (kind == .array and value == .null) return std.json.parseFromSliceLeaky(std.json.Value, alloc, try std.json.Stringify.valueAlloc(alloc, .{
        .op = "literal",
        .type = "sql_array",
        .sql_type = @tagName(element orelse return error.UnsupportedSqlShape),
        .value = @as(?u8, null),
    }, .{}), .{});
    if (kind == .array) return error.UnsupportedSqlShape;
    if (value == .numeric and (kind == .integer or kind == .number)) {
        const target = element orelse if (kind == .integer) @as(@import("array_value.zig").ElementType, .int64) else .float64;
        const source = try @import("schema_expression.zig").numericLiteral(alloc, .{ .string = value.numeric });
        // Keep the exact source and assignment domain separate. Overflow and
        // rounding belong to mutation execution, just as for integer defaults.
        return std.json.parseFromSliceLeaky(std.json.Value, alloc, try std.json.Stringify.valueAlloc(alloc, .{
            .op = "cast",
            .type = if (target == .numeric) "numeric" else @tagName(kind),
            .sql_type = @tagName(target),
            .args = &[_]std.json.Value{source},
        }, .{}), .{ .parse_numbers = false });
    }
    const numeric = (kind == .integer and value == .integer) or (kind == .number and (value == .integer or value == .number));
    if (numeric) {
        const source: ast.ColumnType = if (value == .integer) .integer else .number;
        const source_type: @import("array_value.zig").ElementType = if (value == .integer) (if (std.math.cast(i32, value.integer) != null) .int32 else .int64) else .float64;
        const literal: std.json.Value = if (value == .integer) .{ .integer = value.integer } else .{ .float = value.number };
        const target_type: @import("array_value.zig").ElementType = element orelse if (kind == .integer) .int64 else .float64;
        return std.json.parseFromSliceLeaky(std.json.Value, alloc, try std.json.Stringify.valueAlloc(alloc, .{
            .op = "cast",
            .type = if (target_type == .numeric) "numeric" else @tagName(kind),
            .sql_type = @tagName(target_type),
            .args = &.{.{ .op = "literal", .type = @tagName(source), .sql_type = @tagName(source_type), .value = literal }},
        }, .{}), .{ .parse_numbers = false });
    }
    if (kind == .number and element == .numeric and (value == .null or value == .string))
        return @import("schema_expression.zig").numericLiteral(alloc, if (value == .null) .null else .{ .string = value.string });
    const literal = try bindDefault(alloc, value, kind, element);
    return std.json.parseFromSliceLeaky(std.json.Value, alloc, try std.json.Stringify.valueAlloc(alloc, .{ .op = "literal", .type = if (kind == .uuid) "string" else @tagName(kind), .value = literal }, .{}), .{ .parse_numbers = false });
}

test "SQL view DDL describes without mutation and forwards one exact catalog intent" {
    const Probe = struct {
        calls: usize = 0,
        fn checkpoint(_: *anyopaque) !void {}
        fn ddl(raw: *anyopaque, _: catalog.DdlAllocators, request: catalog.Ddl) !catalog.DdlOutcome {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(.catalog_ddl, std.meta.activeTag(request));
            const command = request.catalog_ddl;
            try std.testing.expectEqual(.view, command.kind);
            if (command.action == .create) {
                try std.testing.expectEqualStrings("SELECT id FROM things", command.view_definition.?.query_sql);
                try std.testing.expect(command.view_definition.?.security_invoker);
            }
            self.calls += 1;
            return .{};
        }
    };
    const cases = [_]struct { sql: []const u8, tag: []const u8 }{
        .{ .sql = "CREATE VIEW v WITH (security_invoker=true) AS SELECT id FROM things", .tag = "CREATE VIEW" },
        .{ .sql = "ALTER VIEW v RENAME TO renamed", .tag = "ALTER VIEW" },
        .{ .sql = "DROP VIEW IF EXISTS v CASCADE", .tag = "DROP VIEW" },
    };
    for (cases) |case| {
        var probe: Probe = .{};
        const backend: catalog.Backend = .{ .ptr = &probe, .vtable = &.{ .resolve = undefined, .scan = undefined, .mutate = undefined, .checkpoint = Probe.checkpoint, .ddl = Probe.ddl } };
        var compiled = try @import("compiler.zig").compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        const bound = try @import("describe.zig").bind(a, backend, &compiled, &.{});
        try std.testing.expect(bound.table == null);
        try std.testing.expectEqual(@as(usize, 0), bound.columns.len);
        try std.testing.expectEqual(@as(usize, 0), probe.calls);
        const output = try execute(.{ .result = a, .scratch = a }, backend, compiled.statement);
        try std.testing.expectEqualStrings(case.tag, output.command_tag);
        try std.testing.expectEqual(@as(usize, 1), probe.calls);
    }
}

test "SQL DDL generated name hints use one bounded allocation and borrow compiler-owned names" {
    const Probe = struct {
        fn run(a: std.mem.Allocator, create: ast.CreateTable) !void {
            const names = try generatedConstraintNamesAlloc(a, create);
            defer a.free(names);
            try std.testing.expectEqual(@as(usize, 2), names.len);
            try std.testing.expect(names[0].ptr == create.constraints[0].generatedConstraintName().?.ptr);
            try std.testing.expect(names[1].ptr == create.constraints[1].generatedConstraintName().?.ptr);
        }
    };
    var compiled = try @import("compiler.zig").compile(std.testing.allocator, "CREATE TABLE t (id BIGINT PRIMARY KEY, email TEXT UNIQUE)", .{});
    defer compiled.deinit();
    try Probe.run(std.testing.allocator, compiled.statement.create_table);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{compiled.statement.create_table});
    var explicit = try @import("compiler.zig").compile(std.testing.allocator, "CREATE TABLE t (id BIGINT CONSTRAINT sql_primary_0 PRIMARY KEY)", .{});
    defer explicit.deinit();
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    const names = try generatedConstraintNamesAlloc(failing.allocator(), explicit.statement.create_table);
    try std.testing.expectEqual(@as(usize, 0), names.len);
}

test "SQL DDL carries generated name provenance outside the editable public schema" {
    const Fake = struct {
        expected: []const []const u8,
        calls: usize = 0,
        fn ddl(raw: *anyopaque, allocators: catalog.DdlAllocators, request: catalog.Ddl) !catalog.DdlOutcome {
            const a = allocators.scratch;
            const self: *@This() = @ptrCast(@alignCast(raw));
            const create = request.create_table;
            try std.testing.expectEqual(self.expected.len, create.generated_constraint_names.len);
            for (self.expected, create.generated_constraint_names) |expected, actual| try std.testing.expectEqualStrings(expected, actual);
            try std.testing.expect(std.mem.indexOf(u8, create.schema_json, "name_generated") == null);
            try std.testing.expect(std.mem.indexOf(u8, create.schema_json, "generated_constraint_names") == null);
            const schema = try std.json.parseFromSlice(std.json.Value, a, create.schema_json, .{});
            defer schema.deinit();
            try std.testing.expectEqualStrings("relational", schema.value.object.get("storage_mode").?.string);
            self.calls += 1;
            return .{ .mutation_outcome = .committed };
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    const cases = [_]struct { sql: []const u8, generated: []const []const u8 }{
        .{ .sql = "CREATE TABLE t (id BIGINT PRIMARY KEY, email TEXT UNIQUE)", .generated = &.{ "sql_primary_0", "sql_unique_1" } },
        .{ .sql = "CREATE TABLE t (id BIGINT CONSTRAINT sql_primary_0 PRIMARY KEY, email TEXT CONSTRAINT sql_unique_1 UNIQUE)", .generated = &.{} },
        .{ .sql = "CREATE TABLE t (id BIGINT, PRIMARY KEY (id), CHECK (id > 0))", .generated = &.{ "sql_constraint_0", "sql_constraint_1" } },
        .{ .sql = "CREATE TABLE t (id BIGINT CHECK (id > 0))", .generated = &.{"sql_inline_0"} },
        .{ .sql = "CREATE TABLE t (id BIGINT PRIMARY KEY, email TEXT CONSTRAINT sql_primary_0 UNIQUE)", .generated = &.{"sql_generated_0"} },
        .{ .sql = "CREATE TABLE t (id BIGINT UNIQUE, CONSTRAINT sql_unique_0 CHECK (id > 0), CONSTRAINT sql_generated_0 CHECK (id < 100))", .generated = &.{"sql_generated_1"} },
        .{ .sql = "CREATE TABLE t (id BIGINT CHECK (id > 0), CONSTRAINT sql_inline_0 UNIQUE (id))", .generated = &.{"sql_generated_0"} },
        .{ .sql = "CREATE TABLE t (id BIGINT, CHECK (id > 0), CONSTRAINT sql_constraint_0 UNIQUE (id))", .generated = &.{"sql_generated_0"} },
        .{ .sql = "CREATE TABLE t (id BIGINT REFERENCES parent (id), CONSTRAINT sql_inline_0 CHECK (id > 0))", .generated = &.{"sql_generated_0"} },
        .{ .sql = "CREATE TABLE t (id BIGINT CONSTRAINT sql_unique_1 UNIQUE, email TEXT UNIQUE)", .generated = &.{"sql_generated_0"} },
    };
    for (cases) |case| {
        var compiled = try @import("compiler.zig").compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        var fake: Fake = .{ .expected = case.generated };
        const result = try execute(.{ .result = arena.allocator(), .scratch = std.testing.allocator }, .{ .ptr = &fake, .vtable = &.{ .resolve = undefined, .scan = undefined, .mutate = undefined, .ddl = Fake.ddl, .checkpoint = Fake.checkpoint } }, compiled.statement);
        try std.testing.expectEqual(@as(usize, 1), fake.calls);
        try std.testing.expectEqualStrings("CREATE TABLE", result.command_tag);
    }
}

test "SQL DDL internal label disambiguation preserves explicit constraints and allocation safety" {
    const Fixture = struct {
        fn run(a: std.mem.Allocator) !void {
            var compiled = try @import("compiler.zig").compile(a, "CREATE TABLE t (id BIGINT PRIMARY KEY, email TEXT CONSTRAINT sql_primary_0 UNIQUE, CONSTRAINT sql_generated_0 CHECK (id > 0))", .{});
            defer compiled.deinit();
            const create = compiled.statement.create_table;
            try std.testing.expectEqualStrings("sql_generated_1", create.constraints[0].generatedConstraintName().?);
            try std.testing.expectEqualStrings("sql_primary_0", create.constraints[1].add_unique.name);
            try std.testing.expect(create.constraints[1].generatedConstraintName() == null);
            try std.testing.expectEqualStrings("sql_generated_0", create.constraints[2].add_check.name);
            try std.testing.expect(create.constraints[2].generatedConstraintName() == null);
            const bytes = try createSchemaAlloc(a, create);
            defer a.free(bytes);
            const parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
            defer parsed.deinit();
            const uniques = parsed.value.object.get("unique_constraints").?.array.items;
            try std.testing.expectEqual(@as(usize, 2), uniques.len);
            try std.testing.expectEqualStrings("sql_generated_1", uniques[0].object.get("name").?.string);
            try std.testing.expectEqualStrings("sql_primary_0", uniques[1].object.get("name").?.string);
            try std.testing.expectEqualStrings("sql_generated_0", parsed.value.object.get("checks").?.array.items[0].object.get("name").?.string);
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL DDL duplicate explicit constraint names remain definite rejection" {
    var compiled = try @import("compiler.zig").compile(std.testing.allocator, "CREATE TABLE t (id BIGINT UNIQUE, CONSTRAINT repeated CHECK (id > 0), CONSTRAINT repeated CHECK (id < 100))", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlConstraintAlreadyExists, createSchemaAlloc(std.testing.allocator, compiled.statement.create_table));
}

test "SQL generated immutable planning cancellation never dispatches CREATE or publishes ALTER candidates" {
    const Control = struct {
        calls: usize = 0,
        dispatches: usize = 0,
        cancel_at: usize = std.math.maxInt(usize),
        fn check(raw: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            const current = self.calls;
            self.calls += 1;
            if (current == self.cancel_at) return error.Cancelled;
        }
        fn checkpoint(raw: *anyopaque) anyerror!void {
            try check(raw);
        }
        fn dispatch(raw: *anyopaque, _: catalog.DdlAllocators, _: catalog.Ddl) !catalog.DdlOutcome {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.dispatches += 1;
            return .{ .mutation_outcome = .committed };
        }
        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .scalar_control = .{ .ptr = self, .checkpoint = check }, .vtable = &.{ .resolve = undefined, .scan = undefined, .mutate = undefined, .ddl = dispatch, .checkpoint = checkpoint } };
        }
    };
    const a = std.testing.allocator;
    var created = try @import("compiler.zig").compile(a, "CREATE TABLE t (s TEXT, g TEXT GENERATED ALWAYS AS (CASE WHEN s IS NULL THEN lower('OK') ELSE lower(s) END) STORED)", .{});
    defer created.deinit();
    var control: Control = .{};
    _ = try execute(.{ .result = a, .scratch = a }, control.backend(), created.statement);
    try std.testing.expectEqual(@as(usize, 1), control.dispatches);
    const create_calls = control.calls;
    for (0..create_calls) |cut| {
        control = .{ .cancel_at = cut };
        try std.testing.expectError(error.Cancelled, execute(.{ .result = a, .scratch = a }, control.backend(), created.statement));
        try std.testing.expectEqual(@as(usize, 0), control.dispatches);
        try std.testing.expectEqual(cut + 1, control.calls);
    }
    var base = try @import("compiler.zig").compile(a, "CREATE TABLE t (s TEXT)", .{});
    defer base.deinit();
    const baseline = try createSchemaAlloc(a, base.statement.create_table);
    defer a.free(baseline);
    var added = try @import("compiler.zig").compile(a, "ALTER TABLE t ADD COLUMN g TEXT GENERATED ALWAYS AS (CASE WHEN s IS NULL THEN lower('OK') ELSE lower(s) END) STORED", .{});
    defer added.deinit();
    var alter_calls: usize = 0;
    var cut: usize = 0;
    while (cut == 0 or cut <= alter_calls) : (cut += 1) {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const owned = arena.allocator();
        var schema = try std.json.parseFromSliceLeaky(std.json.Value, owned, baseline, .{ .parse_numbers = false });
        control = .{ .cancel_at = if (cut == 0) std.math.maxInt(usize) else cut - 1 };
        const limits: @import("immutable_plan.zig").Limits = .{ .checkpoint = Control.check, .checkpoint_context = &control };
        if (cut == 0) {
            try std.testing.expect(try @import("schema_ddl.zig").applyWithLimits(owned, &schema, added.statement.catalog_ddl, limits));
            alter_calls = control.calls;
        } else {
            try std.testing.expectError(error.Cancelled, @import("schema_ddl.zig").applyWithLimits(owned, &schema, added.statement.catalog_ddl, limits));
            try std.testing.expectEqualStrings(baseline, try std.json.Stringify.valueAlloc(owned, schema, .{}));
            try std.testing.expectEqual(cut, control.calls);
        }
    }
}

test "SQL DDL normalizes equivalent index constraints without merging different policies" {
    const cases = [_]struct { sql: []const u8, count: usize, primary: bool = false, explicit: ?[]const u8 = null }{
        .{ .sql = "CREATE TABLE t (id BIGINT PRIMARY KEY UNIQUE)", .count = 1, .primary = true },
        .{ .sql = "CREATE TABLE t (id BIGINT UNIQUE PRIMARY KEY)", .count = 1, .primary = true },
        .{ .sql = "CREATE TABLE t (id BIGINT PRIMARY KEY CONSTRAINT named UNIQUE)", .count = 1, .primary = true, .explicit = "named" },
        .{ .sql = "CREATE TABLE t (id BIGINT CONSTRAINT named UNIQUE PRIMARY KEY)", .count = 1, .primary = true, .explicit = "named" },
        .{ .sql = "CREATE TABLE t (id BIGINT CONSTRAINT first UNIQUE CONSTRAINT second UNIQUE)", .count = 1, .explicit = "first" },
        .{ .sql = "CREATE TABLE t (id BIGINT UNIQUE CONSTRAINT named UNIQUE)", .count = 1, .explicit = "named" },
        .{ .sql = "CREATE TABLE t (a BIGINT, b BIGINT, UNIQUE (a, b), UNIQUE (b, a))", .count = 2 },
        .{ .sql = "CREATE TABLE t (id BIGINT UNIQUE NULLS DISTINCT UNIQUE NULLS NOT DISTINCT)", .count = 2 },
        .{ .sql = "CREATE TABLE t (id BIGINT UNIQUE DEFERRABLE INITIALLY IMMEDIATE UNIQUE DEFERRABLE INITIALLY DEFERRED)", .count = 2 },
        .{ .sql = "CREATE TABLE t (id BIGINT UNIQUE UNIQUE DEFERRABLE)", .count = 2 },
    };
    for (cases) |case| {
        var compiled = try @import("compiler.zig").compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        const create = compiled.statement.create_table;
        try std.testing.expectEqual(case.count, create.constraints.len);
        if (case.count == 1) {
            try std.testing.expectEqual(case.primary, create.constraints[0].add_unique.primary);
            if (case.explicit) |explicit| {
                try std.testing.expectEqualStrings(explicit, create.constraints[0].add_unique.name);
                try std.testing.expect(create.constraints[0].generatedConstraintName() == null);
            }
        }
        const bytes = try createSchemaAlloc(std.testing.allocator, create);
        defer std.testing.allocator.free(bytes);
        const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, bytes, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(case.count, parsed.value.object.get("unique_constraints").?.array.items.len);
    }
}

test "SQL DDL equivalent index normalization transfers explicit provenance with allocation safety" {
    const Fixture = struct {
        fn run(a: std.mem.Allocator) !void {
            var compiled = try @import("compiler.zig").compile(a, "CREATE TABLE t (id BIGINT UNIQUE CONSTRAINT selected UNIQUE PRIMARY KEY, CHECK (id > 0))", .{});
            defer compiled.deinit();
            const create = compiled.statement.create_table;
            try std.testing.expectEqual(@as(usize, 2), create.constraints.len);
            try std.testing.expect(create.constraints[0].add_unique.primary);
            try std.testing.expectEqualStrings("selected", create.constraints[0].add_unique.name);
            try std.testing.expect(create.constraints[0].generatedConstraintName() == null);
            const bytes = try createSchemaAlloc(a, create);
            defer a.free(bytes);
            const names = try generatedConstraintNamesAlloc(a, create);
            defer a.free(names);
            try std.testing.expectEqual(@as(usize, 1), names.len);
            try std.testing.expectEqualStrings(create.constraints[1].generatedConstraintName().?, names[0]);
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL DDL redundant declarations do not amplify published constraint schemas" {
    const a = std.testing.allocator;
    var baseline: ?[]u8 = null;
    defer if (baseline) |bytes| a.free(bytes);
    for ([_]usize{ 64, 1024 }) |count| {
        var sql = std.Io.Writer.Allocating.init(a);
        defer sql.deinit();
        try sql.writer.writeAll("CREATE TABLE t (id BIGINT PRIMARY KEY");
        for (0..count) |index| try sql.writer.print(" CONSTRAINT u{d} UNIQUE", .{index});
        try sql.writer.writeAll(")");
        var compiled = try @import("compiler.zig").compile(a, sql.written(), .{});
        defer compiled.deinit();
        const create = compiled.statement.create_table;
        try std.testing.expectEqual(@as(usize, 1), create.constraints.len);
        try std.testing.expect(create.constraints[0].add_unique.primary);
        try std.testing.expectEqualStrings("u0", create.constraints[0].add_unique.name);
        const bytes = try createSchemaAlloc(a, create);
        if (baseline) |expected| {
            defer a.free(bytes);
            try std.testing.expectEqualStrings(expected, bytes);
        } else baseline = bytes;
    }
}

test "SQL DDL distinct-index normalization growth is allocation safe" {
    const Fixture = struct {
        fn run(a: std.mem.Allocator, source: []const u8) !void {
            var compiled = try @import("compiler.zig").compile(a, source, .{});
            defer compiled.deinit();
            try std.testing.expectEqual(@as(usize, 32), compiled.statement.create_table.constraints.len);
        }
    };
    var sql = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer sql.deinit();
    try sql.writer.writeAll("CREATE TABLE t (");
    for (0..32) |index| try sql.writer.print("{s}c{d} BIGINT UNIQUE", .{ if (index == 0) "" else ", ", index });
    try sql.writer.writeAll(")");
    try Fixture.run(std.testing.allocator, sql.written());
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{sql.written()});
}

test "SQL TRUNCATE lowers complete table set and honest durable admission receipt" {
    const Fake = struct {
        fn ddl(_: *anyopaque, _: catalog.DdlAllocators, request: catalog.Ddl) !catalog.DdlOutcome {
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
    const result = try execute(.{ .result = std.testing.allocator, .scratch = std.testing.allocator }, .{ .ptr = &sentinel, .vtable = &.{ .resolve = undefined, .scan = undefined, .mutate = undefined, .ddl = Fake.ddl, .checkpoint = Fake.checkpoint } }, compiled.statement);
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
    const source = defaults[1].object.get("expression").?.object.get("args").?.array.items[0];
    try std.testing.expectEqualStrings("numeric", source.object.get("type").?.string);
    try std.testing.expectEqualStrings("0.1", source.object.get("value").?.string);
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
        "ALTER TABLE exprs ADD COLUMN rounded real DEFAULT CAST(0.1+0.2 AS double precision)",
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

test "SQL array DDL preserves precise element identity through CREATE and ALTER" {
    var create = try @import("compiler.zig").compile(std.testing.allocator, "CREATE TABLE arrays (a int4[2][3])", .{});
    defer create.deinit();
    const created = try createSchemaAlloc(std.testing.allocator, create.statement.create_table);
    defer std.testing.allocator.free(created);
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    var base = try @import("compiler.zig").compile(alloc, "CREATE TABLE arrays (id bigint)", .{});
    const candidate = try std.json.parseFromSliceLeaky(std.json.Value, alloc, created, .{});
    const properties = try @import("schema_columns.zig").properties(candidate);
    const column = try @import("schema_columns.zig").column("a", properties.object.get("a").?);
    try std.testing.expectEqual(.array, column.type);
    try std.testing.expectEqual(.int32, column.element_type.?);
    defer base.deinit();
    const original = try createSchemaAlloc(alloc, base.statement.create_table);
    var schema = try std.json.parseFromSliceLeaky(std.json.Value, alloc, original, .{});
    var alter = try @import("compiler.zig").compile(alloc, "ALTER TABLE arrays ADD COLUMN a jsonb[]", .{});
    defer alter.deinit();
    _ = try @import("schema_ddl.zig").apply(alloc, &schema, alter.statement.catalog_ddl);
    const altered = try @import("schema_columns.zig").properties(schema);
    const added = try @import("schema_columns.zig").column("a", altered.object.get("a").?);
    try std.testing.expectEqual(.array, added.type);
    try std.testing.expectEqual(.jsonb, added.element_type.?);
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
        "DROP INDEX items_id",
        "CREATE UNIQUE INDEX items_lower_title ON items ((lower(title))) WHERE title IS NOT NULL",
        "DROP INDEX items_lower_title",
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

fn uniqueNullSchema(alloc: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var created = try @import("compiler.zig").compile(alloc, "CREATE TABLE items (tenant BIGINT NOT NULL, email TEXT UNIQUE NULLS NOT DISTINCT, status TEXT)", .{});
    defer created.deinit();
    const bytes = try createSchemaAlloc(alloc, created.statement.create_table);
    defer alloc.free(bytes);
    var schema = try std.json.parseFromSliceLeaky(std.json.Value, a, bytes, .{});
    for ([_][]const u8{
        "ALTER TABLE items ADD CONSTRAINT tenant_email UNIQUE NULLS NOT DISTINCT (tenant, email) DEFERRABLE INITIALLY DEFERRED",
        "CREATE UNIQUE INDEX folded_email ON items (lower(email)) NULLS NOT DISTINCT WHERE status='active'",
        "CREATE UNIQUE INDEX ordinary_email ON items (email) NULLS DISTINCT",
    }) |sql| {
        var compiled = try @import("compiler.zig").compile(alloc, sql, .{});
        defer compiled.deinit();
        try std.testing.expect(try @import("schema_ddl.zig").apply(a, &schema, compiled.statement.catalog_ddl));
    }
    const encoded = try std.json.Stringify.valueAlloc(a, schema, .{});
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(alloc, encoded);
    defer parsed.deinit(alloc);
    const definitions = try parsed.relationalUniqueDefinitions(a);
    try std.testing.expectEqual(@as(usize, 4), definitions.len);
    for (definitions[0..3]) |definition| try std.testing.expect(definition.nulls_not_distinct);
    try std.testing.expect(!definitions[3].nulls_not_distinct);
    try std.testing.expect(definitions[1].deferrable);
    try std.testing.expectEqual(.deferred, definitions[1].timing);
    try std.testing.expectEqual(@as(usize, 1), definitions[2].keys.len);
    try std.testing.expectEqual(@as(usize, 1), definitions[2].where.len);
    const wire = schema.object.get("unique_constraints").?.array.items;
    try std.testing.expectEqualStrings("index", wire[2].object.get("origin").?.string);
    try std.testing.expectEqualStrings("index", wire[3].object.get("origin").?.string);
}

test "SQL UNIQUE null semantics survive public schema lowering and allocation failure" {
    try uniqueNullSchema(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, uniqueNullSchema, .{});
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

test "SQL boolean partial index predicates normalize without losing NULL membership" {
    const compiler = @import("compiler.zig");
    const ddl = @import("schema_ddl.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var create = try compiler.compile(a, "CREATE TABLE threads (archived BOOLEAN NOT NULL, optional BOOLEAN, preview TEXT)", .{});
    defer create.deinit();
    const original = try createSchemaAlloc(a, create.statement.create_table);
    const cases = [_]struct { sql: []const u8, column: []const u8 = "archived", op: []const u8 = "eq", value: bool }{
        .{ .sql = "archived", .value = true },
        .{ .sql = "NOT archived", .value = false },
        .{ .sql = "NOT NOT archived", .value = true },
        .{ .sql = "archived IS TRUE", .value = true },
        .{ .sql = "archived IS FALSE", .value = false },
        .{ .sql = "archived IS NOT TRUE", .op = "is_distinct", .value = true },
        .{ .sql = "archived IS NOT FALSE", .op = "is_distinct", .value = false },
        .{ .sql = "NOT (archived = true)", .value = false },
        .{ .sql = "NOT (archived <> true)", .value = true },
        .{ .sql = "NOT (archived IS FALSE)", .op = "is_distinct", .value = false },
        .{ .sql = "optional", .column = "optional", .value = true },
        .{ .sql = "NOT optional", .column = "optional", .value = false },
        .{ .sql = "optional IS TRUE", .column = "optional", .value = true },
        .{ .sql = "optional IS FALSE", .column = "optional", .value = false },
        .{ .sql = "NOT (optional = true)", .column = "optional", .value = false },
        .{ .sql = "optional IS NOT TRUE", .column = "optional", .op = "is_distinct", .value = true },
        .{ .sql = "optional IS NOT FALSE", .column = "optional", .op = "is_distinct", .value = false },
        .{ .sql = "NOT (optional IS TRUE)", .column = "optional", .op = "is_distinct", .value = true },
        .{ .sql = "NOT (optional IS NOT FALSE)", .column = "optional", .value = false },
    };
    for (cases) |case| {
        var schema = try std.json.parseFromSliceLeaky(std.json.Value, a, original, .{});
        var compiled = try compiler.compile(a, try std.fmt.allocPrint(a, "CREATE INDEX visible ON threads(preview) WHERE {s}", .{case.sql}), .{});
        defer compiled.deinit();
        try std.testing.expect(try ddl.apply(a, &schema, compiled.statement.catalog_ddl));
        const conditions = schema.object.get("relational_indexes").?.array.items[0].object.get("where").?.array.items;
        try std.testing.expectEqual(@as(usize, 1), conditions.len);
        try std.testing.expectEqualStrings(case.column, conditions[0].object.get("column").?.string);
        try std.testing.expectEqualStrings(case.op, conditions[0].object.get("op").?.string);
        try std.testing.expectEqual(case.value, conditions[0].object.get("value").?.bool);
    }
    for ([_][]const u8{ "archived AND preview <> ''", "NOT (NOT archived OR preview = '')" }) |predicate| {
        var schema = try std.json.parseFromSliceLeaky(std.json.Value, a, original, .{});
        var compiled = try compiler.compile(a, try std.fmt.allocPrint(a, "CREATE UNIQUE INDEX visible ON threads(preview) WHERE {s}", .{predicate}), .{});
        defer compiled.deinit();
        try std.testing.expect(try ddl.apply(a, &schema, compiled.statement.catalog_ddl));
        const index_where = schema.object.get("relational_indexes").?.array.items[0].object.get("where").?;
        const unique_where = schema.object.get("unique_constraints").?.array.items[0].object.get("where").?;
        try std.testing.expectEqual(@as(usize, 2), index_where.array.items.len);
        try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(a, index_where, .{}), try std.json.Stringify.valueAlloc(a, unique_where, .{}));
        try std.testing.expectEqualStrings("eq", index_where.array.items[0].object.get("op").?.string);
        try std.testing.expectEqual(true, index_where.array.items[0].object.get("value").?.bool);
        try std.testing.expectEqualStrings("ne", index_where.array.items[1].object.get("op").?.string);
    }
}

test "SQL boolean partial indexes reject nonboolean columns and unsupported disjunctions" {
    const compiler = @import("compiler.zig");
    const ddl = @import("schema_ddl.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var create = try compiler.compile(a, "CREATE TABLE threads (archived BOOLEAN, preview TEXT)", .{});
    defer create.deinit();
    const original = try createSchemaAlloc(a, create.statement.create_table);
    for ([_][]const u8{ "preview", "NOT preview", "preview IS FALSE" }) |predicate| {
        var schema = try std.json.parseFromSliceLeaky(std.json.Value, a, original, .{});
        var compiled = try compiler.compile(a, try std.fmt.allocPrint(a, "CREATE INDEX visible ON threads(preview) WHERE {s}", .{predicate}), .{});
        defer compiled.deinit();
        try std.testing.expectError(error.SqlTypeMismatch, ddl.apply(a, &schema, compiled.statement.catalog_ddl));
    }
    for ([_][]const u8{ "archived OR preview = ''", "NOT (archived AND preview = '')" }) |predicate| {
        var schema = try std.json.parseFromSliceLeaky(std.json.Value, a, original, .{});
        var compiled = try compiler.compile(a, try std.fmt.allocPrint(a, "CREATE INDEX visible ON threads(preview) WHERE {s}", .{predicate}), .{});
        defer compiled.deinit();
        try std.testing.expectError(error.UnsupportedSqlShape, ddl.apply(a, &schema, compiled.statement.catalog_ddl));
    }
}
