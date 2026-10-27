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

const std = @import("std");
const sql_parser = @import("sql_parser");
const lexer = sql_parser.lexer;
const token = sql_parser.token;
pub const ast = @import("ast.zig");
pub const Diagnostic = @import("diagnostics.zig").Diagnostic;

pub const Limits = struct {
    max_bytes: usize = @import("resource_limits.zig").default_memory_bytes,
    max_tokens: usize = 16_384,
    max_nodes: usize = 8_192,
    max_depth: usize = 64,
    max_parameters: u32 = 1_024,
    max_insert_rows: usize = 1_000,
};

pub const Error = std.mem.Allocator.Error || error{
    InvalidSqlSyntax,
    UnsupportedSqlShape,
    SqlLimitExceeded,
    InvalidSqlParameter,
    InvalidSqlNumber,
    DuplicateSqlColumn,
    DuplicateSqlAssignment,
    SqlInvalidUnicodeEscape,
    SqlInvalidTextEncoding,
    SqlInvalidParameterValue,
    SqlInvalidTextRepresentation,
    SqlNumericOutOfRange,
    SqlInvalidFunctionDefinition,
};

test "compiler sequence originals preserve typed options and never bind ownership during parsing" {
    const a = std.testing.allocator;
    var corpus = try @import("parity_fixtures.zig").Corpus.init(a);
    defer corpus.deinit();
    for ([_][]const u8{ "sql-0311", "sql-0312", "sql-0313", "sql-0314", "sql-0315", "sql-0316", "sql-0317" }, 0..) |id, index| {
        const original = try corpus.get(id);
        var compiled = try compile(a, original.sql, .{});
        defer compiled.deinit();
        try std.testing.expectEqual(@as(u32, 0), compiled.parameter_count);
        const ddl = compiled.statement.catalog_ddl;
        try std.testing.expectEqual(.sequence, ddl.kind);
        try std.testing.expectEqualStrings("usage_records_id_seq", ddl.name.table);
        try std.testing.expectEqual(index == 2 or index == 4 or index == 6, ddl.conditional);
        if (index < 3) {
            try std.testing.expectEqual(.create, ddl.action);
            try std.testing.expectEqual(@as(?i64, 1), ddl.sequence_options.?.values.start);
        } else if (index < 5) try std.testing.expectEqual(.alter_sequence, ddl.action) else {
            try std.testing.expectEqual(.drop, ddl.action);
            try std.testing.expectEqual(index == 6, ddl.cascade);
            try std.testing.expectEqual(@as(usize, 1), ddl.sequence_targets.len);
            continue;
        }
        const options = ddl.sequence_options.?;
        try std.testing.expectEqual(.unchanged, options.values.owned_by);
        if (index == 1) {
            const owner = options.owned_by.column;
            try std.testing.expectEqualStrings("public", owner.table.namespace.?);
            try std.testing.expectEqualStrings("usage_records", owner.table.table);
            try std.testing.expectEqualStrings("id", owner.column);
            try std.testing.expectError(error.InvalidSequenceDefinition, options.bind(.{}));
            const bound = try options.bind(.{ .integer = .int64, .owner = .{ .table_id = 42, .column_id = 19 } });
            try std.testing.expectEqual(@as(u64, 19), bound.owned_by.column.column_id);
        } else if (index == 2) {
            try std.testing.expectEqual(@as(?i64, 32), options.cache);
            try std.testing.expectEqual(@as(?bool, false), options.values.cycle);
        } else if (index == 3) {
            try std.testing.expectEqual(@as(?i64, 1000), options.values.restart.?.value);
        } else if (index == 4) {
            try std.testing.expectEqualStrings("integer", options.data_type.?.table);
            try std.testing.expectEqual(.none, options.owned_by);
            try std.testing.expectEqual(.none, (try options.bind(.{ .integer = .int32 })).owned_by);
        }
    }
}

test "compiler sequence options preserve full width reset intent and qualified owned names" {
    const a = std.testing.allocator;
    var compiled = try compile(a, "CREATE SEQUENCE IF NOT EXISTS \"Tenant\".\"Counters\" AS int8 INCREMENT BY -9223372036854775808 MINVALUE -9223372036854775808 MAXVALUE 9223372036854775807 START WITH 100 RESTART WITH 7 CACHE 9223372036854775807 NO CYCLE OWNED BY \"default\".public.usage_records.id", .{});
    defer compiled.deinit();
    const ddl = compiled.statement.catalog_ddl;
    try std.testing.expectEqualStrings("Tenant", ddl.name.namespace.?);
    try std.testing.expectEqualStrings("Counters", ddl.name.table);
    const options = ddl.sequence_options.?;
    try std.testing.expectEqual(@as(?i64, std.math.minInt(i64)), options.values.increment);
    try std.testing.expectEqual(std.math.minInt(i64), options.values.minimum.value);
    try std.testing.expectEqual(std.math.maxInt(i64), options.values.maximum.value);
    try std.testing.expectEqual(@as(?i64, std.math.maxInt(i64)), options.cache);
    try std.testing.expectEqualStrings("default", options.owned_by.column.table.database.?);
    const sequence = @import("../system_catalog/sequences.zig");
    const record = try sequence.Record.create(try options.bind(.{ .integer = .int64, .owner = .{ .table_id = 9, .column_id = 77 } }));
    try std.testing.expectEqual(@as(i64, 100), record.definition.start);
    try std.testing.expectEqual(@as(i64, 7), record.state.last_value);
    try std.testing.expect(!record.state.is_called);
    var reset = try compile(a, "ALTER SEQUENCE public.counter RESTART NO MINVALUE NO MAXVALUE", .{});
    defer reset.deinit();
    try std.testing.expectEqual(@as(?i64, null), reset.statement.catalog_ddl.sequence_options.?.values.restart.?.value);
    try std.testing.expectEqual(.default, reset.statement.catalog_ddl.sequence_options.?.values.minimum);
    var drop = try compile(a, "DROP SEQUENCE IF EXISTS one, public.two, db.tenant.three CASCADE", .{});
    defer drop.deinit();
    try std.testing.expectEqual(@as(usize, 3), drop.statement.catalog_ddl.sequence_targets.len);
    try std.testing.expectEqualStrings("db", drop.statement.catalog_ddl.sequence_targets[2].database.?);
    var rename = try compile(a, "ALTER SEQUENCE IF EXISTS public.one RENAME TO two", .{});
    defer rename.deinit();
    try std.testing.expectEqual(.rename, rename.statement.catalog_ddl.action);
    try std.testing.expectEqualStrings("two", rename.statement.catalog_ddl.new_name.?);
}

test "compiler sequence options reject duplicates malformed integers and ambiguous ownership" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "CREATE SEQUENCE s START 1 START 2",        "CREATE SEQUENCE s CACHE 2 CACHE 3",
        "CREATE SEQUENCE s CYCLE NO CYCLE",         "CREATE SEQUENCE s NO MINVALUE MINVALUE 1",
        "CREATE SEQUENCE s MAXVALUE 9 NO MAXVALUE", "CREATE SEQUENCE s INCREMENT 1 INCREMENT 2",
        "CREATE SEQUENCE s AS bigint AS integer",   "CREATE SEQUENCE s OWNED BY NONE OWNED BY t.id",
        "CREATE SEQUENCE s RESTART 7 RESTART",      "CREATE SEQUENCE s OWNED BY t",
        "CREATE SEQUENCE s OWNED BY a.b.c.d.e",     "CREATE SEQUENCE s CACHE $1",
        "CREATE SEQUENCE s RESTART WITH",           "ALTER SEQUENCE s",
    }) |sql| try std.testing.expectError(error.InvalidSqlSyntax, compile(a, sql, .{}));
    for ([_][]const u8{ "CREATE SEQUENCE s INCREMENT 1.0", "CREATE SEQUENCE s INCREMENT 1e0" }) |sql|
        try std.testing.expectError(error.SqlInvalidTextRepresentation, compile(a, sql, .{}));
    for ([_][]const u8{ "CREATE SEQUENCE s START 9223372036854775808", "CREATE SEQUENCE s START -9223372036854775809" }) |sql|
        try std.testing.expectError(error.SqlNumericOutOfRange, compile(a, sql, .{}));
}

test "compiler sequence binding retains catalog resolution and late value validation" {
    const a = std.testing.allocator;
    var compiled = try compile(a, "ALTER SEQUENCE missing AS tenant.integer CACHE -1", .{});
    defer compiled.deinit();
    const options = compiled.statement.catalog_ddl.sequence_options.?;
    try std.testing.expectEqualStrings("tenant", options.data_type.?.namespace.?);
    try std.testing.expectEqualStrings("integer", options.data_type.?.table);
    try std.testing.expectEqual(@as(?i64, -1), options.cache);
    try std.testing.expectError(error.InvalidSequenceDefinition, options.bind(.{ .integer = .int32 }));
    var declaration = try compile(a, "CREATE SEQUENCE counter AS pg_catalog.int4 OWNED BY tenant.records.id", .{});
    defer declaration.deinit();
    var intent = declaration.statement.catalog_ddl.sequence_options.?;
    try std.testing.expectEqualStrings("pg_catalog", intent.data_type.?.namespace.?);
    try std.testing.expectError(error.InvalidSequenceDefinition, intent.bind(.{}));
    try std.testing.expectError(error.InvalidSequenceDefinition, intent.bind(.{ .owner = .{ .table_id = 8, .column_id = 3 } }));
    try std.testing.expectError(error.InvalidSequenceDefinition, intent.bind(.{ .integer = .int32, .owner = .{ .table_id = 0, .column_id = 3 } }));
    const resolved = try intent.bind(.{ .integer = .int32, .owner = .{ .table_id = 8, .column_id = 3 } });
    try std.testing.expectEqual(@as(?@import("../system_catalog/sequences.zig").Integer, .int32), resolved.integer);
    intent.values.owned_by = .none;
    try std.testing.expectError(error.InvalidSequenceDefinition, intent.bind(.{ .integer = .int32, .owner = .{ .table_id = 8, .column_id = 3 } }));
}

test "compiler sequence type intent distinguishes quoted names from grammar aliases" {
    for ([_]struct { sql: []const u8, quoted: bool }{
        .{ .sql = "CREATE SEQUENCE counter AS INTEGER", .quoted = false },
        .{ .sql = "CREATE SEQUENCE counter AS \"integer\"", .quoted = true },
    }) |example| {
        var compiled = try compile(std.testing.allocator, example.sql, .{});
        defer compiled.deinit();
        const intent = compiled.statement.catalog_ddl.sequence_options.?;
        try std.testing.expectEqualStrings("integer", intent.data_type.?.table);
        try std.testing.expectEqual(example.quoted, intent.data_type_quoted);
    }
    const invalid: ast.SequenceOptions = .{ .data_type_quoted = true };
    try std.testing.expectError(error.InvalidSequenceDefinition, invalid.bind(.{}));
}

test "compiler sequence intent owns names and releases every failed allocation" {
    const Harness = struct {
        fn run(a: std.mem.Allocator) !void {
            var buffer: [256]u8 = undefined;
            const sql = try std.fmt.bufPrint(&buffer, "CREATE SEQUENCE tenant.\"Counter\" AS pg_catalog.int8 CACHE 32 OWNED BY tenant.\"Orders\".\"Id\"", .{});
            var compiled = try compile(a, sql, .{});
            defer compiled.deinit();
            @memset(sql, 'x');
            const ddl = compiled.statement.catalog_ddl;
            try std.testing.expectEqualStrings("Counter", ddl.name.table);
            try std.testing.expectEqualStrings("pg_catalog", ddl.sequence_options.?.data_type.?.namespace.?);
            try std.testing.expectEqualStrings("Orders", ddl.sequence_options.?.owned_by.column.table.table);
            try std.testing.expectEqualStrings("Id", ddl.sequence_options.?.owned_by.column.column);
            var drop = try compile(a, "DROP SEQUENCE one, tenant.two, db.tenant.three RESTRICT", .{});
            defer drop.deinit();
            try std.testing.expectEqual(@as(usize, 3), drop.statement.catalog_ddl.sequence_targets.len);
        }
    };
    try Harness.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL document table option grammar preserves quoted names and rejects lossy mixtures" {
    const a = std.testing.allocator;
    var compiled = try compile(a,
        \\CREATE TABLE public.docs () WITH (antfly.default_type='Doc', antfly.storage_mode='document')
        \\DOCUMENT SCHEMA "Doc" AS JSON '{"type":"object","properties":{"title":{"type":"text"}}}'
    , .{});
    defer compiled.deinit();
    try std.testing.expectEqualStrings("public", compiled.statement.create_table.table.namespace.?);
    try std.testing.expectEqualStrings("Doc", compiled.statement.create_table.document.?.schemas[0].name);
    const cases = .{
        .{ error.InvalidSqlSyntax, "CREATE TABLE docs(id text,)" },
        .{ error.InvalidSqlSyntax, "CREATE TABLE docs () DOCUMENT SCHEMA doc AS JSON '{}'" },
        .{ error.InvalidSqlSyntax, "CREATE TABLE docs () WITH (antfly.default_type='doc')" },
        .{ error.InvalidSqlSyntax, "CREATE TABLE docs () WITH (antfly.storage_mode='document',antfly.enforce_types='true')" },
        .{ error.UnsupportedSqlShape, "CREATE TABLE docs(id text) WITH (antfly.storage_mode='document')" },
        .{ error.UnsupportedSqlShape, "CREATE TABLE docs () WITH (antfly.storage_mode='relational')" },
        .{ error.UnsupportedSqlShape, "CREATE TABLE docs () WITH (antfly.unknown='document')" },
        .{ error.SqlInvalidParameterValue, "CREATE TABLE docs () WITH (antfly.storage_mode='document',antfly.storage_mode='document')" },
        .{ error.SqlInvalidParameterValue, "CREATE TABLE docs () WITH (antfly.storage_mode='document',antfly.default_type='missing')" },
        .{ error.SqlInvalidParameterValue, "CREATE TABLE docs () WITH (antfly.storage_mode='document') DOCUMENT SCHEMA doc AS JSON '{}' DOCUMENT SCHEMA doc AS JSON '{}'" },
        .{ error.SqlInvalidParameterValue, "CREATE TABLE docs () WITH (antfly.storage_mode='document') DOCUMENT SCHEMA doc AS JSON '{}'" },
        .{ error.UnsupportedSqlShape, "CREATE TABLE docs () WITH (antfly.storage_mode='document',antfly.default_type='doc') DOCUMENT SCHEMA doc AS JSON '{}' DOCUMENT SCHEMA invoice AS JSON '{}'" },
    };
    inline for (cases) |case| try std.testing.expectError(case[0], compile(a, case[1], .{}));
}

test "SQL routine declaration admission consumes grammar before missing-body diagnostics" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "CREATE FUNCTION f() RETURNS trigger LANGUAGE plpgsql",
        "CREATE OR REPLACE FUNCTION f() RETURNS trigger LANGUAGE plpgsql EXTERNAL SECURITY DEFINER SET search_path TO public",
        "CREATE PROCEDURE p() LANGUAGE plpgsql",
    }) |sql| try std.testing.expectError(error.SqlInvalidFunctionDefinition, compile(a, sql, .{}));
    for ([_][]const u8{
        "CREATE FUNCTION f() RETURNS trigger LANGUAGE plpgsql TRANSFORM FOR TYPE jsonb, public.hstore",
        "CREATE FUNCTION f() RETURNS trigger LANGUAGE plpgsql SECURITY",
        "CREATE FUNCTION f() RETURNS trigger LANGUAGE plpgsql; SELECT 1",
        "CREATE FUNCTION f() RETURNS trigger LANGUAGE plpgsql STABLE VOLATILE",
    }) |sql| try std.testing.expectError(error.InvalidSqlSyntax, compile(a, sql, .{}));
    // A valid body is not an activated routine. Unknown languages/signatures
    // and SUPPORT names require authoritative catalog resolution, not guesses.
    for ([_][]const u8{
        "CREATE FUNCTION f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$",
        "CREATE FUNCTION f() RETURNS trigger LANGUAGE unknown_language",
        "CREATE FUNCTION f(arg text) RETURNS text LANGUAGE sql AS $$SELECT arg$$",
        "CREATE FUNCTION f() RETURNS trigger LANGUAGE plpgsql SUPPORT missing_support",
    }) |sql| try std.testing.expectError(error.UnsupportedSqlShape, compile(a, sql, .{}));
}

/// Immutable and schema independent: safely share a compiled statement between
/// readers, binding each execution against its own catalog snapshot and values.
/// Move-only owner; every string, literal, and node lives in this one arena.
pub const Compiled = struct {
    arena: std.heap.ArenaAllocator,
    statement: ast.Statement,
    parameter_count: u32,
    uses_current_setting: bool = false,
    uses_sequences: bool = false,

    pub fn deinit(self: *Compiled) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub fn compile(allocator: std.mem.Allocator, sql: []const u8, limits: Limits) Error!Compiled {
    var diagnostic: Diagnostic = .{};
    return compileDiagnostic(allocator, sql, limits, &diagnostic);
}

pub const CompiledScalar = struct {
    arena: std.heap.ArenaAllocator,
    expression: *const ast.Scalar,
    parameter_count: u32,

    pub fn deinit(self: *CompiledScalar) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub fn compileScalar(allocator: std.mem.Allocator, sql: []const u8, limits: Limits) Error!CompiledScalar {
    if (sql.len > limits.max_bytes) return error.SqlLimitExceeded;
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();
    const lexed = lexer.tokenizeBoundedDiagnosticAlloc(scratch.allocator(), sql, limits.max_tokens) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.SqlTokenLimitExceeded => return error.SqlLimitExceeded,
        else => return error.InvalidSqlSyntax,
    };
    const tokens = switch (lexed) {
        .tokens => |value| value,
        .diagnostic => |failure| return lexicalError(failure.kind),
    };
    var diagnostic: Diagnostic = .{};
    var parser: Parser = .{ .alloc = arena.allocator(), .tokens = tokens.items, .source = sql, .limits = limits, .diagnostic = &diagnostic };
    const expression = try parser.scalar(0, 0);
    if (parser.pos != tokens.items.len) return error.InvalidSqlSyntax;
    try parser.checkScalarDepth(expression, 0);
    return .{ .arena = arena, .expression = expression, .parameter_count = parser.parameter_count };
}

pub fn compileDiagnostic(allocator: std.mem.Allocator, sql: []const u8, limits: Limits, diagnostic: *Diagnostic) Error!Compiled {
    diagnostic.* = .{};
    if (sql.len > limits.max_bytes) {
        diagnostic.message = "SQL statement byte budget exceeded";
        return error.SqlLimitExceeded;
    }
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const alloc = arena.allocator();
    // Token buffers and decoded lexer strings are preparation scratch. Cached
    // plans retain only semantic data, not comments/source text/token capacity.
    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();
    const result = lexer.tokenizeBoundedDiagnosticAlloc(scratch.allocator(), sql, limits.max_tokens) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.SqlTokenLimitExceeded => {
            diagnostic.message = "SQL token budget exceeded";
            return error.SqlLimitExceeded;
        },
        else => return error.InvalidSqlSyntax,
    };
    const tokens = switch (result) {
        .tokens => |tokens| tokens,
        .diagnostic => |failure| {
            diagnostic.* = .{ .start = failure.source_start, .end = failure.source_end, .message = failure.message() };
            return lexicalError(failure.kind);
        },
    };
    if (tokens.items.len > limits.max_tokens) return error.SqlLimitExceeded;
    var parser: Parser = .{ .alloc = alloc, .tokens = tokens.items, .source = sql, .limits = limits, .diagnostic = diagnostic };
    const statement = parser.statement() catch |err| {
        // An unsupported table producer must not hide a definite grammar
        // error later in its arguments. Keep this bounded, allocation-free
        // validation off the supported-query hot path. Quoted names and
        // string contents are never reinterpreted as reserved identifiers.
        if (err == error.UnsupportedSqlShape) {
            try parser.validateReservedNamedArguments();
            try parser.validateUnsupportedDdlGrammar();
        }
        return err;
    };
    _ = parser.take(.semicolon);
    const tail_statement = if (statement == .explain) statement.explain.statement.* else statement;
    if (parser.pos != tokens.items.len) {
        try parser.validateReservedNamedArguments();
        try parser.validateUnsupportedDdlGrammar();
        return parser.fail(switch (tail_statement) {
            // PostgreSQL row locks belong to a SELECT producer, not to the
            // mutation itself. Likewise UPDATE/DELETE do not accept ORDER/LIMIT.
            // Never execute a valid mutation prefix while discarding its suffix.
            .update, .delete => if (tokens.items[parser.pos].isKeyword(.@"for") or
                tokens.items[parser.pos].isKeyword(.order) or tokens.items[parser.pos].isKeyword(.limit) or
                tokens.items[parser.pos].isKeyword(.offset) or tokens.items[parser.pos].isKeyword(.fetch))
                error.InvalidSqlSyntax
            else
                error.UnsupportedSqlShape,
            else => error.UnsupportedSqlShape,
        }, "unexpected trailing SQL; only one supported statement is allowed");
    }
    return .{ .arena = arena, .statement = statement, .parameter_count = parser.parameter_count, .uses_current_setting = parser.uses_current_setting, .uses_sequences = parser.uses_sequences };
}

fn lexicalError(kind: lexer.LexErrorKind) Error {
    return switch (kind) {
        .invalid_escape_sequence => error.SqlInvalidUnicodeEscape,
        .invalid_string_encoding => error.SqlInvalidTextEncoding,
        else => error.InvalidSqlSyntax,
    };
}

/// NUMBER tokens already have PostgreSQL separator validation from the lexer.
/// Zig accepts separators between digits, but not immediately after a radix
/// prefix; remove only that permitted separator without allocating a copy.
fn parseIntegerToken(comptime T: type, text: []const u8) std.fmt.ParseIntError!T {
    const radix: u8 = if (text.len > 2 and text[0] == '0') switch (text[1]) {
        'x', 'X' => 16,
        'o', 'O' => 8,
        'b', 'B' => 2,
        else => 0,
    } else 0;
    if (radix == 0) return std.fmt.parseInt(T, text, 10);
    const start: usize = if (text[2] == '_') 3 else 2;
    return std.fmt.parseInt(T, text[start..], radix);
}

test "SQL PostgreSQL nondecimal integer literals preserve exact signed values" {
    const queries = [_][]const u8{ "0x_6", "0o_6", "0b_110", "0x6e0", "0XFF_FF", "-0x_8000_0000_0000_0000", "1_000" };
    const expected = [_]i64{ 6, 6, 6, 1760, 65535, std.math.minInt(i64), 1000 };
    for (queries, expected) |sql, value_| {
        var compiled = try compileScalar(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        try std.testing.expectEqual(value_, compiled.expression.literal.integer);
    }
}

test "SQL transaction timestamp syntax retains labels quoted columns and unsupported warning boundaries" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "SELECT CURRENT_TIMESTAMP", "SELECT CURRENT_TIMESTAMP(6)", "SELECT CURRENT_TIMESTAMP(0x6)", "SELECT CURRENT_TIMESTAMP(0x_6)", "SELECT CURRENT_TIMESTAMP(0o6)", "SELECT CURRENT_TIMESTAMP(0b110)" }) |sql| {
        var compiled = try compile(a, sql, .{});
        defer compiled.deinit();
        try std.testing.expectEqualStrings("current_timestamp", compiled.statement.select.columns[0].alias.?);
        try std.testing.expectEqualStrings("$current_timestamp", compiled.statement.select.columns[0].expression.?.call.name);
    }
    var quoted = try compileScalar(a, "\"current_timestamp\"", .{});
    defer quoted.deinit();
    try std.testing.expectEqualStrings("current_timestamp", quoted.expression.column);
    for ([_][]const u8{ "CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP(-1)", "CURRENT_TIMESTAMP(1.5)", "CURRENT_TIMESTAMP($1)", "CURRENT_TIMESTAMP(1 + 1)" }) |sql| try std.testing.expectError(error.InvalidSqlSyntax, compileScalar(a, sql, .{}));
    try std.testing.expectError(error.UnsupportedSqlShape, compileScalar(a, "CURRENT_TIMESTAMP(7)", .{}));
    try std.testing.expectError(error.UnsupportedSqlShape, compileScalar(a, "CURRENT_TIMESTAMP(0xe)", .{}));
    try std.testing.expectError(error.UnsupportedSqlShape, compileScalar(a, "\"$current_timestamp\"(6)", .{}));
}

test "SQL escape string diagnostics retain PostgreSQL SQLSTATEs" {
    const a = std.testing.allocator;
    try std.testing.expectError(error.SqlInvalidUnicodeEscape, compileScalar(a, "E'\\u12'", .{}));
    try std.testing.expectError(error.SqlInvalidTextEncoding, compileScalar(a, "E'\\xFF'", .{}));
    try std.testing.expectError(error.InvalidSqlSyntax, compileScalar(a, "E'\\u0000'", .{}));
    try std.testing.expectError(error.InvalidSqlSyntax, compileScalar(a, "E'a' 'b'", .{}));
    try std.testing.expectError(error.InvalidSqlSyntax, compileScalar(a, "E'a'\nE'b'", .{}));
    try std.testing.expectError(error.InvalidSqlSyntax, compileScalar(a, "E'a'\n/* comment */'b'", .{}));
    var diagnostic: Diagnostic = .{};
    try std.testing.expectError(error.SqlInvalidUnicodeEscape, compileDiagnostic(a, "SELECT E'\\u12'", .{}, &diagnostic));
    try std.testing.expect(diagnostic.start >= 7);
    try std.testing.expect(diagnostic.end <= 14);
}

test "setting authority capture follows parsed calls, not SQL text" {
    var literal = try compile(std.testing.allocator, "SELECT 'current_setting(' AS note /* current_setting('app.tenant') */", .{});
    defer literal.deinit();
    try std.testing.expect(!literal.uses_current_setting);
    var call = try compile(std.testing.allocator, "SELECT current_setting('app.tenant')", .{});
    defer call.deinit();
    try std.testing.expect(call.uses_current_setting);
}

test "SQL MERGE compiles ordered matched and unmatched mutation arms" {
    // sql-0579, sql-0581, sql-0583, sql-0589: syntax/AST coverage only;
    // these cases remain unresolved until native atomic execution is wired.
    const sql = "MERGE INTO usage_records AS target USING source_records AS source ON target.id = source.id " ++
        "WHEN MATCHED AND target.status = 'locked' THEN DO NOTHING " ++
        "WHEN MATCHED AND lower(source.status) != lower(target.status) THEN UPDATE SET status = lower(source.status) " ++
        "WHEN MATCHED AND source.status = 'deleted' THEN DELETE " ++
        "WHEN NOT MATCHED AND source.status = 'ready' THEN INSERT (id, status) VALUES (source.id, source.status) " ++
        "WHEN NOT MATCHED THEN DO NOTHING RETURNING target.id, target.status";
    var compiled = try compile(std.testing.allocator, sql, .{});
    defer compiled.deinit();
    try std.testing.expect(compiled.statement == .merge);
    const merge_statement = compiled.statement.merge;
    try std.testing.expectEqualStrings("usage_records", merge_statement.table.table);
    try std.testing.expectEqualStrings("target", merge_statement.alias.?);
    try std.testing.expect(merge_statement.source.* == .table);
    try std.testing.expectEqualStrings("source", merge_statement.source.table.alias.?);
    try std.testing.expectEqual(@as(usize, 5), merge_statement.arms.len);
    try std.testing.expect(merge_statement.arms[0].matched and merge_statement.arms[0].action == .nothing);
    try std.testing.expectEqualStrings("status", merge_statement.arms[1].action.update[0].field);
    try std.testing.expect(merge_statement.arms[2].action == .delete);
    try std.testing.expect(!merge_statement.arms[3].matched);
    try std.testing.expectEqual(@as(usize, 2), merge_statement.arms[3].action.insert.columns.len);
    try std.testing.expect(merge_statement.arms[4].action == .nothing);
    try std.testing.expectEqual(@as(usize, 2), merge_statement.returning.?.len);
}

test "SQL MERGE rejects invalid arm/action and insert shapes before execution" {
    const alloc = std.testing.allocator;
    for ([_][]const u8{
        "MERGE INTO t USING s ON t.id = s.id",
        "MERGE INTO t USING s ON t.id = s.id WHEN NOT MATCHED THEN DELETE",
        "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN INSERT (id) VALUES (s.id)",
        "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET id = s.id, id = 3",
        "MERGE INTO t USING s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id)",
        "MERGE INTO t USING s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id, s.id)",
    }) |sql| {
        if (compile(alloc, sql, .{})) |result| {
            var compiled = result;
            compiled.deinit();
            return error.TestUnexpectedResult;
        } else |err| try std.testing.expect(err == error.InvalidSqlSyntax or err == error.DuplicateSqlColumn or err == error.DuplicateSqlAssignment or err == error.UnsupportedSqlShape);
    }
}

test "SQL MERGE preserves explicit DEFAULT cells for native preparation" {
    var compiled = try compile(std.testing.allocator, "MERGE INTO t USING s ON t.id=s.id WHEN MATCHED THEN UPDATE SET status=DEFAULT WHEN NOT MATCHED THEN INSERT (id,status) VALUES (s.id,DEFAULT)", .{});
    defer compiled.deinit();
    try std.testing.expect(compiled.statement.merge.arms[0].action.update[0].use_default);
    try std.testing.expect(compiled.statement.merge.arms[1].action.insert.values[1] == null);
}

test "SQL MERGE follows PostgreSQL CTE recursion grammar and preserves derived sources" {
    // PostgreSQL forbids the outer WITH RECURSIVE clause on MERGE, even
    // when a particular seed would produce no recursive delta.
    const sql = "WITH RECURSIVE source_rows AS (SELECT id, status FROM usage_records " ++
        "UNION ALL SELECT child.id, child.status FROM usage_records AS child " ++
        "JOIN source_rows AS parent ON child.organization_id = parent.id) " ++
        "MERGE INTO usage_records USING source_rows ON usage_records.id = source_rows.id " ++
        "WHEN MATCHED THEN UPDATE SET status = source_rows.status";
    try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, sql, .{}));
    try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, "WITH RECURSIVE source_rows AS (SELECT id FROM source) MERGE INTO target USING source_rows ON target.id=source_rows.id WHEN MATCHED THEN DELETE", .{}));
    var compiled = try compile(std.testing.allocator, "WITH source_rows AS (SELECT id,status FROM usage_records) MERGE INTO usage_records USING source_rows ON usage_records.id=source_rows.id WHEN MATCHED THEN UPDATE SET status=source_rows.status", .{});
    defer compiled.deinit();
    try std.testing.expect(compiled.statement == .merge);
    try std.testing.expectEqual(@as(usize, 1), compiled.statement.merge.ctes.len);
    try std.testing.expect(!compiled.statement.merge.ctes[0].recursive);
    try std.testing.expect(compiled.statement.merge.source.* == .table);
    try std.testing.expectEqualStrings("source_rows", compiled.statement.merge.source.table.name.table);

    var derived = try compile(std.testing.allocator, "MERGE INTO t USING (SELECT id FROM s) AS source ON t.id = source.id WHEN MATCHED THEN DELETE", .{});
    defer derived.deinit();
    try std.testing.expect(derived.statement.merge.source.* == .derived);
    var inner = try compile(std.testing.allocator, "MERGE INTO target USING (WITH RECURSIVE walk AS (SELECT id FROM source UNION ALL SELECT child.id FROM source child JOIN walk parent ON child.parent_id=parent.id) SELECT id FROM walk) AS source ON target.id=source.id WHEN MATCHED THEN DELETE", .{});
    defer inner.deinit();
    try std.testing.expect(inner.statement.merge.source.derived.query.ctes[0].recursive);
}

test "SQL INSERT VALUES subquery source is flat at row budget" {
    const alloc = std.testing.allocator;
    var sql: std.ArrayList(u8) = .empty;
    defer sql.deinit(alloc);
    try sql.appendSlice(alloc, "INSERT INTO things (_id,n) VALUES ");
    for (0..1000) |index| {
        const row = try std.fmt.allocPrint(alloc, "{s}('r{d}',{s})", .{ if (index == 0) "" else ",", index, if (index == 0) "(SELECT 7)" else "1" });
        defer alloc.free(row);
        try sql.appendSlice(alloc, row);
    }
    var compiled = try compile(alloc, sql.items, .{});
    defer compiled.deinit();
    try std.testing.expect(compiled.statement == .insert);
    const source = compiled.statement.insert.source orelse return error.TestUnexpectedResult;
    try std.testing.expect(source.generated_values);
    try std.testing.expect(source.set_operation == null);
    try std.testing.expectEqual(@as(usize, 1000), source.values_arms.len);
    for (source.values_arms) |arm| try std.testing.expectEqual(@as(usize, 2), arm.columns.len);
}

test "compiler treats TIMESTAMPTZ quoted literals as typed datetime casts" {
    var compiled = try compile(std.testing.allocator, "INSERT INTO usage_records (id, created_at_ns) VALUES ('u_typed_time', TIMESTAMPTZ '2025-01-01T01:30:00+01:30') RETURNING created_at_ns", .{});
    defer compiled.deinit();
    try std.testing.expect(compiled.statement == .insert);
    const expression = compiled.statement.insert.expressions[0][1] orelse return error.TestUnexpectedResult;
    try std.testing.expect(expression.* == .cast);
    try std.testing.expectEqual(ast.ColumnType.datetime, expression.cast.type);
    try std.testing.expectEqualStrings("2025-01-01T01:30:00+01:30", expression.cast.operand.literal.string);
}

const Parser = struct {
    alloc: std.mem.Allocator,
    tokens: []const token.Token,
    source: []const u8,
    limits: Limits,
    diagnostic: *Diagnostic,
    pos: usize = 0,
    node_count: usize = 0,
    parameter_count: u32 = 0,
    uses_current_setting: bool = false,
    uses_sequences: bool = false,
    relation_depth: usize = 0,

    fn fail(self: *Parser, err: Error, message: []const u8) Error {
        const current = if (self.pos < self.tokens.len) self.tokens[self.pos] else null;
        self.diagnostic.* = .{
            .start = if (current) |t| t.source_start else self.source.len,
            .end = if (current) |t| t.source_end else self.source.len,
            .message = message,
        };
        return err;
    }

    /// Inspect the whole unsupported ALTER before returning a feature error.
    /// An earlier supported action cannot hide invalid later grammar or execute
    /// a prefix. Keywords are lexer tokens: quoted identifiers/text do not match.
    fn validateUnsupportedDdlGrammar(self: *Parser) Error!void {
        if (self.tokens.len < 3 or !self.tokens[0].isKeyword(.alter) or !self.tokens[1].isKeyword(.table)) return;
        var depth: usize = 0;
        for (self.tokens, 0..) |current, index| {
            if (current.kind == .lparen) depth += 1;
            if (current.kind == .rparen) depth -|= 1;
            if (depth == 0 and index + 2 < self.tokens.len and current.isKeyword(.add) and
                self.tokens[index + 1].isKeyword(.period) and self.tokens[index + 2].isKeyword(.@"for"))
            {
                self.pos = index + 1;
                return self.fail(error.InvalidSqlSyntax, "PostgreSQL does not accept ADD PERIOD FOR; use a range column");
            }
        }
    }

    fn validateReservedNamedArguments(self: *Parser) Error!void {
        for (self.tokens, 0..) |current, i| {
            if (current.owned or current.keyword == null or token.keywordClass(current.keyword.?) != .reserved or i + 2 >= self.tokens.len) continue;
            if (self.tokens[i + 1].kind == .eq and self.tokens[i + 2].kind == .gt) {
                self.pos = i;
                return self.fail(error.InvalidSqlSyntax, "reserved named arguments must be quoted");
            }
        }
    }

    fn node(self: *Parser) Error!void {
        if (self.node_count >= self.limits.max_nodes) return self.fail(error.SqlLimitExceeded, "SQL AST node budget exceeded");
        self.node_count += 1;
    }

    fn peek(self: *Parser, kind: token.TokenKind) bool {
        return self.pos < self.tokens.len and self.tokens[self.pos].kind == kind;
    }

    fn take(self: *Parser, kind: token.TokenKind) bool {
        if (!self.peek(kind)) return false;
        self.pos += 1;
        return true;
    }

    fn keyword(self: *Parser, word: token.TokenKeyword) bool {
        if (self.pos >= self.tokens.len or !self.tokens[self.pos].isKeyword(word)) return false;
        self.pos += 1;
        return true;
    }

    fn expect(self: *Parser, kind: token.TokenKind) Error!void {
        if (!self.take(kind)) return self.fail(error.InvalidSqlSyntax, "expected SQL punctuation");
    }

    fn expectKeyword(self: *Parser, word: token.TokenKeyword) Error!void {
        if (!self.keyword(word)) return self.fail(error.InvalidSqlSyntax, "expected SQL keyword");
    }

    fn identifier(self: *Parser) Error![]const u8 {
        if (!self.peek(.identifier)) return self.fail(error.InvalidSqlSyntax, "expected SQL identifier");
        const t = self.tokens[self.pos];
        if (std.mem.indexOfScalar(u8, t.text, 0) != null) return self.fail(error.InvalidSqlSyntax, "SQL identifiers cannot contain NUL bytes");
        if (t.keyword) |word| {
            if (token.keywordClass(word) == .reserved) return self.fail(error.InvalidSqlSyntax, "reserved keyword must be quoted when used as an identifier");
        }
        self.pos += 1;
        // Quoted identifiers preserve case; ordinary identifiers follow SQL's
        // ASCII folding rule. The lexer owns already-unescaped quoted strings.
        if (t.owned) return try self.alloc.dupe(u8, t.text);
        const folded = try self.alloc.dupe(u8, t.text);
        _ = std.ascii.lowerString(folded, folded);
        return folded;
    }

    pub fn name(self: *Parser) Error!ast.Name {
        const first = try self.identifier();
        if (!self.take(.dot)) return .{ .table = first };
        const second = try self.identifier();
        if (!self.take(.dot)) return .{ .namespace = first, .table = second };
        return .{ .database = first, .namespace = second, .table = try self.identifier() };
    }

    fn tableReferenceName(self: *Parser) Error!ast.Name {
        // Inheritance is not a catalog capability: every table reference is
        // already limited to its exact table. Accept ONLY at that boundary,
        // not in generic object names such as a new table declaration.
        _ = self.keyword(.only);
        return self.name();
    }

    fn field(self: *Parser) Error![]const u8 {
        var result = try self.identifier();
        // Internal separator preserves the distinction between t.column and
        // the single quoted identifier "t.column" without reparsing names.
        var parts: usize = 1;
        while (self.take(.dot)) {
            if (parts == 4) return self.fail(error.InvalidSqlSyntax, "column references have at most four identifiers");
            result = try std.fmt.allocPrint(self.alloc, "{s}\x00{s}", .{ result, try self.identifier() });
            parts += 1;
        }
        return result;
    }

    /// Assignment indirection is a column root plus field selectors, not a
    /// relation-qualified read reference. Retain every selector for typed
    /// binding; concatenate once and charge each selector to the parser budget.
    fn assignmentField(self: *Parser) Error![]const u8 {
        const first = try self.identifier();
        if (!self.peek(.dot)) return first;
        var bytes: std.ArrayList(u8) = .empty;
        try bytes.appendSlice(self.alloc, first);
        var depth: usize = 1;
        while (self.take(.dot)) {
            try self.node();
            if (depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "assignment field nesting exceeds configured bound");
            try bytes.append(self.alloc, 0);
            try bytes.appendSlice(self.alloc, try self.identifier());
            depth += 1;
        }
        return bytes.toOwnedSlice(self.alloc);
    }

    fn value(self: *Parser) Error!ast.Value {
        try self.node();
        if (self.keyword(.null)) return .null;
        if (self.keyword(.true)) return .{ .boolean = true };
        if (self.keyword(.false)) return .{ .boolean = false };
        if (self.peek(.placeholder)) {
            const t = self.tokens[self.pos];
            self.pos += 1;
            const index = std.fmt.parseInt(u32, t.text[1..], 10) catch return self.fail(error.InvalidSqlParameter, "invalid positional parameter");
            if (index == 0 or index > self.limits.max_parameters) return self.fail(error.InvalidSqlParameter, "parameter index exceeds configured bound");
            self.parameter_count = @max(self.parameter_count, index);
            return .{ .parameter = index };
        }
        if (self.peek(.string)) {
            const t = self.tokens[self.pos];
            self.pos += 1;
            return .{ .string = try self.alloc.dupe(u8, t.text) };
        }
        const negative = self.take(.minus);
        if (!negative) _ = self.take(.plus);
        if (!self.peek(.number)) return self.fail(if (self.pos == self.tokens.len or self.peek(.semicolon) or self.peek(.rparen)) error.InvalidSqlSyntax else error.UnsupportedSqlShape, "expected a literal or positional parameter; expression is not supported");
        const t = self.tokens[self.pos];
        self.pos += 1;
        const radix = t.text.len > 2 and t.text[0] == '0' and std.mem.indexOfScalar(u8, "xXoObB", t.text[1]) != null;
        if (!radix and std.mem.indexOfAny(u8, t.text, ".eE") != null) return self.numericLiteral(t.text, negative);
        const magnitude = parseIntegerToken(u64, t.text) catch return self.numericLiteral(t.text, negative);
        if (negative) {
            if (magnitude > @as(u64, std.math.maxInt(i64)) + 1) return self.numericLiteral(t.text, negative);
            if (magnitude == @as(u64, std.math.maxInt(i64)) + 1) return .{ .integer = std.math.minInt(i64) };
            return .{ .integer = -@as(i64, @intCast(magnitude)) };
        }
        if (magnitude > std.math.maxInt(i64)) return self.numericLiteral(t.text, negative);
        return .{ .integer = @intCast(magnitude) };
    }

    fn numericLiteral(self: *Parser, text: []const u8, negative: bool) Error!ast.Value {
        return .{ .numeric = if (negative) try std.fmt.allocPrint(self.alloc, "-{s}", .{text}) else try self.alloc.dupe(u8, text) };
    }

    fn scalarNode(self: *Parser, expression: ast.Scalar) Error!*const ast.Scalar {
        try self.node();
        const node_value = try self.alloc.create(ast.Scalar);
        node_value.* = expression;
        return node_value;
    }

    fn singleColumnSubquery(self: *Parser, query: ast.Select) Error!void {
        // Explicit projection arity is known before catalog binding. Reject
        // invalid scalar/IN/quantified subqueries even inside mutations, whose
        // source relation must not be opened just to discover this error.
        // SELECT * remains a binder check because its width is schema-bound.
        if (query.set_operation) |set| {
            try self.singleColumnSubquery(set.left.*);
            try self.singleColumnSubquery(set.right.*);
            return;
        }
        if (query.columns.len > 1) return self.fail(error.InvalidSqlSyntax, "subquery must return one column in this expression");
    }

    fn rowSubqueryWidth(self: *Parser, query: ast.Select, width: usize) Error!void {
        if (query.set_operation) |set| {
            try self.rowSubqueryWidth(set.left.*, width);
            try self.rowSubqueryWidth(set.right.*, width);
            return;
        }
        // Wildcards have a schema-bound width. Preserve an exact output
        // contract for the binder instead of guessing from AST item count.
        if (!query.count_all) {
            if (query.columns.len == 0) return;
            for (query.columns) |projection| if (projection.wildcard) return;
        }
        if ((if (query.count_all) @as(usize, 1) else query.columns.len) != width)
            return self.fail(error.InvalidSqlSyntax, "row subquery column count differs from assignment targets");
    }

    fn rowSubqueryAssignments(self: *Parser, targets: []const []const u8, assignments: *std.ArrayList(ast.Assignment)) Error!void {
        self.relation_depth += 1;
        defer self.relation_depth -= 1;
        if (self.relation_depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL subquery nesting limit exceeded");
        const nested = try self.statement();
        if (nested != .select) return self.fail(error.InvalidSqlSyntax, "row assignment requires SELECT");
        try self.expect(.rparen);
        const query = try self.alloc.create(ast.Select);
        query.* = nested.select;
        try self.rowSubqueryWidth(query.*, targets.len);
        query.required_output_columns = targets.len;
        // Every member points at one producer identity. Lowering attaches that
        // producer once in the target's lexical frame, preserving correlation
        // and positional typing without inventing an independent CTE scope.
        for (targets, 0..) |target, index| {
            const slot = try self.scalarNode(.{ .literal = .{ .integer = @intCast(index) } });
            const expression = try self.scalarNode(.{ .call = .{ .name = "$row_scalar", .args = try self.alloc.dupe(*const ast.Scalar, &.{slot}), .subquery = query } });
            try assignments.append(self.alloc, .{ .field = target, .expression = expression });
        }
    }

    fn scalar(self: *Parser, depth: usize, minimum: u8) Error!*const ast.Scalar {
        if (depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL scalar nesting budget exceeded");
        if (self.peek(.lbracket)) return self.fail(error.InvalidSqlSyntax, "bare array brackets require ARRAY constructor context");
        var left: *const ast.Scalar = undefined;
        if (self.keyword(.not)) {
            left = try self.scalarNode(.{ .unary = .{ .op = .not, .operand = try self.scalar(depth + 1, 3) } });
        } else if (self.keyword(.exists)) {
            self.relation_depth += 1;
            defer self.relation_depth -= 1;
            if (self.relation_depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL subquery nesting limit exceeded");
            try self.expect(.lparen);
            const query = try self.alloc.create(ast.Select);
            const nested = try self.statement();
            if (nested != .select) return self.fail(error.InvalidSqlSyntax, "subquery requires SELECT");
            query.* = nested.select;
            try self.expect(.rparen);
            left = try self.scalarNode(.{ .call = .{ .name = "$exists", .args = &.{}, .subquery = query } });
        } else if (self.take(.lparen)) {
            if (self.pos < self.tokens.len and (self.tokens[self.pos].isKeyword(.select) or self.tokens[self.pos].isKeyword(.with))) {
                self.relation_depth += 1;
                defer self.relation_depth -= 1;
                if (self.relation_depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL subquery nesting limit exceeded");
                const query = try self.alloc.create(ast.Select);
                const nested = try self.statement();
                if (nested != .select) return self.fail(error.InvalidSqlSyntax, "subquery requires SELECT");
                query.* = nested.select;
                try self.singleColumnSubquery(query.*);
                left = try self.scalarNode(.{ .call = .{ .name = "$scalar", .args = &.{}, .subquery = query } });
            } else {
                left = try self.scalar(depth + 1, 0);
                if (self.take(.comma)) {
                    var fields: std.ArrayList(*const ast.Scalar) = .empty;
                    try fields.append(self.alloc, left);
                    while (true) {
                        if (fields.items.len == 256) return self.fail(error.SqlLimitExceeded, "row expression exceeds column limit");
                        try fields.append(self.alloc, try self.scalar(depth + 1, 0));
                        if (!self.take(.comma)) break;
                    }
                    left = try self.scalarNode(.{ .call = .{ .name = "$row", .args = try fields.toOwnedSlice(self.alloc) } });
                }
            }
            try self.expect(.rparen);
        } else if (self.keyword(.cast)) {
            try self.expect(.lparen);
            const operand = try self.scalar(depth + 1, 0);
            try self.expectKeyword(.as);
            const kind = try self.castType();
            try self.expect(.rparen);
            left = try self.scalarNode(.{ .cast = .{ .operand = operand, .type = kind.type, .element_type = kind.element_type, .numeric_modifier = kind.numeric_modifier, .temporal_kind = kind.temporal_kind } });
        } else if (self.peek(.identifier) and !self.tokens[self.pos].owned and std.ascii.eqlIgnoreCase(self.tokens[self.pos].text, "array") and self.pos + 1 < self.tokens.len and self.tokens[self.pos + 1].kind == .lbracket) {
            self.pos += 1;
            left = try self.arrayConstructor(depth + 1);
        } else if (self.peek(.identifier) and !self.tokens[self.pos].owned and (std.ascii.eqlIgnoreCase(self.tokens[self.pos].text, "timestamptz") or std.ascii.eqlIgnoreCase(self.tokens[self.pos].text, "interval") or std.ascii.eqlIgnoreCase(self.tokens[self.pos].text, "date") or std.ascii.eqlIgnoreCase(self.tokens[self.pos].text, "timestamp") or std.ascii.eqlIgnoreCase(self.tokens[self.pos].text, "time") or std.ascii.eqlIgnoreCase(self.tokens[self.pos].text, "timetz")) and self.pos + 1 < self.tokens.len and self.tokens[self.pos + 1].kind == .string) {
            // Typed literals use the same validating/canonicalizing cast as
            // CAST(text AS timestamptz), including offset normalization.
            const temporal_kind: ?@import("temporal_value.zig").Kind = if (std.ascii.eqlIgnoreCase(self.tokens[self.pos].text, "interval")) .interval else if (std.ascii.eqlIgnoreCase(self.tokens[self.pos].text, "date")) .date else if (std.ascii.eqlIgnoreCase(self.tokens[self.pos].text, "timestamp")) .timestamp else if (std.ascii.eqlIgnoreCase(self.tokens[self.pos].text, "time")) .time else if (std.ascii.eqlIgnoreCase(self.tokens[self.pos].text, "timetz")) .time_tz else null;
            self.pos += 1;
            const literal = try self.scalarNode(.{ .literal = try self.value() });
            left = try self.scalarNode(.{ .cast = .{ .operand = literal, .type = .datetime, .temporal_kind = temporal_kind } });
        } else if (self.peek(.identifier) and !self.tokens[self.pos].owned and std.ascii.eqlIgnoreCase(self.tokens[self.pos].text, "current_user")) {
            self.pos += 1;
            if (self.peek(.lparen)) return self.fail(error.InvalidSqlSyntax, "CURRENT_USER does not take parentheses");
            self.uses_current_setting = true;
            left = try self.scalarNode(.{ .call = .{ .name = "$current_user", .args = &.{} } });
        } else if (self.keyword(.current_date)) {
            if (self.peek(.lparen)) return self.fail(error.InvalidSqlSyntax, "CURRENT_DATE does not take parentheses");
            left = try self.scalarNode(.{ .call = .{ .name = "$current_date", .args = &.{} } });
        } else if (self.keyword(.current_timestamp)) {
            var precision: i64 = 6;
            if (self.take(.lparen)) {
                if (!self.peek(.number)) return self.fail(error.InvalidSqlSyntax, "CURRENT_TIMESTAMP precision requires an integer literal");
                const text = self.tokens[self.pos].text;
                const radix = text.len > 2 and text[0] == '0' and std.mem.indexOfScalar(u8, "xXoObB", text[1]) != null;
                if (!radix and std.mem.indexOfAny(u8, text, ".eE") != null) return self.fail(error.InvalidSqlSyntax, "CURRENT_TIMESTAMP precision requires an integer literal");
                precision = parseIntegerToken(i64, text) catch return self.fail(error.UnsupportedSqlShape, "CURRENT_TIMESTAMP precision is outside the supported domain");
                self.pos += 1;
                try self.expect(.rparen);
                // PostgreSQL clamps >6 with a WARNING. Do not silently omit
                // that observable contract until notice delivery is wired.
                if (precision > 6) return self.fail(error.UnsupportedSqlShape, "CURRENT_TIMESTAMP precision warnings are not yet supported");
            }
            const arg = try self.scalarNode(.{ .literal = .{ .integer = precision } });
            left = try self.scalarNode(.{ .call = .{ .name = "$current_timestamp", .args = try self.alloc.dupe(*const ast.Scalar, &.{arg}) } });
        } else if (self.keyword(.case)) {
            const base = if (self.pos < self.tokens.len and self.tokens[self.pos].isKeyword(.when)) null else try self.scalar(depth + 1, 0);
            var branches: std.ArrayList(ast.Scalar.Branch) = .empty;
            while (self.keyword(.when)) {
                var condition = try self.scalar(depth + 1, 0);
                if (base) |base_expression| condition = try self.scalarNode(.{ .binary = .{ .op = .eq, .left = base_expression, .right = condition } });
                try self.expectKeyword(.then);
                try branches.append(self.alloc, .{ .condition = condition, .value = try self.scalar(depth + 1, 0) });
            }
            if (branches.items.len == 0) return self.fail(error.InvalidSqlSyntax, "CASE requires WHEN");
            const otherwise = if (self.keyword(.@"else")) try self.scalar(depth + 1, 0) else null;
            try self.expectKeyword(.end);
            left = try self.scalarNode(.{ .case_when = .{ .branches = try branches.toOwnedSlice(self.alloc), .otherwise = otherwise } });
        } else if ((self.peek(.minus) or self.peek(.plus)) and !(self.pos + 1 < self.tokens.len and self.tokens[self.pos + 1].kind == .number and
            (self.pos + 2 >= self.tokens.len or self.tokens[self.pos + 2].kind != .colon_colon)))
        {
            // A postfix cast binds before unary sign. Folding a signed token
            // first would admit overflowing positive casts and erase -0.0.
            // Plain signed literals still use value() for exact int64 minima.
            const negative = self.take(.minus);
            if (!negative) try self.expect(.plus);
            left = try self.scalarNode(.{ .unary = .{ .op = if (negative) .negative else .positive, .operand = try self.scalar(depth + 1, 7) } });
        } else if (self.peek(.identifier) and self.pos + 1 < self.tokens.len and (self.tokens[self.pos + 1].kind == .lparen or
            (self.pos + 3 < self.tokens.len and self.tokens[self.pos + 1].kind == .dot and self.tokens[self.pos + 2].kind == .identifier and self.tokens[self.pos + 3].kind == .lparen)))
        {
            const namespace = if (self.tokens[self.pos + 1].kind == .dot) blk: {
                const scope_name = try self.identifier();
                try self.expect(.dot);
                break :blk scope_name;
            } else null;
            const function = self.tokens[self.pos];
            self.pos += 2;
            const function_name = try self.alloc.dupe(u8, function.text);
            if (!function.owned) _ = std.ascii.lowerString(function_name, function_name);
            if (std.mem.startsWith(u8, function_name, "$")) return self.fail(error.UnsupportedSqlShape, "compiler-owned helper functions cannot be called from SQL");
            // Preserve namespace identity: a quoted single identifier containing
            // a dot must never impersonate a schema-qualified builtin.
            const name_value = if (namespace) |scope| try std.fmt.allocPrint(self.alloc, "{s}\x00{s}", .{ scope, function_name }) else function_name;
            if ((namespace == null or std.mem.eql(u8, namespace.?, "pg_catalog")) and std.mem.eql(u8, function_name, "current_setting")) self.uses_current_setting = true;
            if (namespace == null or std.mem.eql(u8, namespace.?, "pg_catalog")) {
                inline for (.{ "nextval", "currval", "lastval", "setval" }) |sequence_function| {
                    if (std.mem.eql(u8, function_name, sequence_function)) self.uses_sequences = true;
                }
            }
            var args: std.ArrayList(*const ast.Scalar) = .empty;
            var within_group: ?*const ast.Scalar.WithinGroup = null;
            const distinct = self.keyword(.distinct);
            const explicit_all = !distinct and self.keyword(.all);
            if (explicit_all and !function.owned and namespace == null) inline for (.{ "coalesce", "greatest", "least", "nullif", "row" }) |special| {
                if (std.mem.eql(u8, name_value, special)) return self.fail(error.InvalidSqlSyntax, "ALL is not valid in this SQL construct");
            };
            const star = self.take(.star);
            if (star) {
                if (!std.mem.eql(u8, name_value, "count")) return self.fail(error.InvalidSqlSyntax, "only COUNT accepts a star argument");
                try self.expect(.rparen);
            } else if (!function.owned and std.mem.eql(u8, name_value, "extract")) {
                const unit = if (self.peek(.string)) try self.value() else blk: {
                    if (!self.peek(.identifier)) return self.fail(error.InvalidSqlSyntax, "EXTRACT requires a field name");
                    const unit_text = self.tokens[self.pos].text;
                    self.pos += 1;
                    break :blk ast.Value{ .string = unit_text };
                };
                try self.expectKeyword(.from);
                try args.append(self.alloc, try self.scalarNode(.{ .literal = unit }));
                try args.append(self.alloc, try self.scalar(depth + 1, 0));
                try self.expect(.rparen);
            } else if (!function.owned and std.mem.eql(u8, name_value, "position")) {
                // IN is otherwise a comparison operator. Parse its first
                // operand above comparison precedence, then swap SQL's order
                // into the shared strpos(text, needle) bound instruction.
                const needle = try self.scalar(depth + 1, 4);
                try self.expectKeyword(.in);
                const haystack = try self.scalar(depth + 1, 0);
                try args.appendSlice(self.alloc, &.{ haystack, needle });
                try self.expect(.rparen);
            } else if (!self.take(.rparen)) {
                const first = try self.scalar(depth + 1, 0);
                try args.append(self.alloc, first);
                if (!function.owned and std.mem.eql(u8, name_value, "substring") and (self.keyword(.from) or (self.pos < self.tokens.len and self.tokens[self.pos].isKeyword(.@"for")))) {
                    if (self.keyword(.@"for")) {
                        try args.append(self.alloc, try self.scalarNode(.{ .literal = .{ .integer = 1 } }));
                        try args.append(self.alloc, try self.scalar(depth + 1, 0));
                    } else {
                        try args.append(self.alloc, try self.scalar(depth + 1, 0));
                        if (self.keyword(.@"for")) try args.append(self.alloc, try self.scalar(depth + 1, 0));
                    }
                } else if (!function.owned and std.mem.eql(u8, name_value, "overlay") and self.ddlWord("placing")) {
                    try args.append(self.alloc, try self.scalar(depth + 1, 0));
                    try self.expectKeyword(.from);
                    try args.append(self.alloc, try self.scalar(depth + 1, 0));
                    if (self.keyword(.@"for")) try args.append(self.alloc, try self.scalar(depth + 1, 0));
                } else while (self.take(.comma)) {
                    try args.append(self.alloc, try self.scalar(depth + 1, 0));
                }
                if (self.pos < self.tokens.len and self.tokens[self.pos].isKeyword(.order)) {
                    const orders = try self.orderExpressionsDepth(false, depth + 1);
                    const ordering = try self.alloc.alloc(ast.Scalar.Ordering, orders.len);
                    for (orders, ordering) |order, *out| {
                        out.* = .{ .descending = order.descending, .nulls_first = order.nulls_first };
                        try args.append(self.alloc, order.expression orelse try self.scalarNode(.{ .column = order.field }));
                    }
                    const descriptor = try self.alloc.create(ast.Scalar.WithinGroup);
                    descriptor.* = .{ .orders = ordering, .ordinary = true };
                    within_group = descriptor;
                }
                try self.expect(.rparen);
            }
            if (self.ddlWord("within")) {
                if (within_group != null) return self.fail(error.InvalidSqlSyntax, "aggregate ORDER BY and WITHIN GROUP cannot be combined");
                try self.expectKeyword(.group);
                try self.expect(.lparen);
                const orders = try self.orderExpressionsDepth(false, depth + 1);
                if (orders.len == 0) return self.fail(error.InvalidSqlSyntax, "WITHIN GROUP requires ORDER BY");
                try self.expect(.rparen);
                const ordering = try self.alloc.alloc(ast.Scalar.Ordering, orders.len);
                for (orders, ordering) |order, *out| {
                    out.* = .{ .descending = order.descending, .nulls_first = order.nulls_first };
                    try args.append(self.alloc, order.expression orelse try self.scalarNode(.{ .column = order.field }));
                }
                const descriptor = try self.alloc.create(ast.Scalar.WithinGroup);
                descriptor.* = .{ .orders = ordering };
                within_group = descriptor;
            }
            var filter: ?*const ast.Scalar = null;
            if (self.keyword(.filter)) {
                try self.expect(.lparen);
                try self.expectKeyword(.where);
                filter = try self.scalar(depth + 1, 0);
                try self.expect(.rparen);
            }
            const window_spec = if (self.keyword(.over)) try self.window(depth + 1) else null;
            const row = !function.owned and std.mem.eql(u8, name_value, "row");
            if (row and (distinct or star or filter != null or window_spec != null or within_group != null)) return self.fail(error.InvalidSqlSyntax, "ROW constructor does not accept aggregate modifiers");
            if (row and args.items.len > 256) return self.fail(error.SqlLimitExceeded, "row expression exceeds column limit");
            left = try self.scalarNode(.{ .call = .{ .name = if (row) "$row" else name_value, .args = try args.toOwnedSlice(self.alloc), .star = star, .distinct = distinct, .filter = filter, .window = window_spec, .within_group = within_group } });
        } else if (self.peek(.identifier) and !self.tokens[self.pos].isKeyword(.null) and !self.tokens[self.pos].isKeyword(.true) and !self.tokens[self.pos].isKeyword(.false)) {
            left = try self.scalarNode(.{ .column = try self.field() });
        } else left = try self.scalarNode(.{ .literal = try self.value() });

        while (self.pos < self.tokens.len) {
            if (minimum <= 3) {
                const negated = self.tokens[self.pos].isKeyword(.not) and self.pos + 1 < self.tokens.len and (self.tokens[self.pos + 1].isKeyword(.in) or self.tokens[self.pos + 1].isKeyword(.between) or self.tokens[self.pos + 1].isKeyword(.like) or self.tokens[self.pos + 1].isKeyword(.ilike));
                if (negated) self.pos += 1;
                if (self.keyword(.between)) {
                    const symmetric = self.ddlWord("symmetric");
                    if (!symmetric) _ = self.ddlWord("asymmetric");
                    const low = try self.scalar(depth + 1, 4);
                    try self.expectKeyword(.@"and");
                    const high = try self.scalar(depth + 1, 4);
                    const operand = left;
                    left = try self.scalarNode(.{ .binary = .{ .op = .@"and", .left = try self.scalarNode(.{ .binary = .{ .op = .gte, .left = operand, .right = low } }), .right = try self.scalarNode(.{ .binary = .{ .op = .lte, .left = operand, .right = high } }) } });
                    // Preserve PostgreSQL's comparison/three-valued-logic
                    // expansion. LEAST/GREATEST would discard NULL bounds.
                    if (symmetric) {
                        const reversed = try self.scalarNode(.{ .binary = .{ .op = .@"and", .left = try self.scalarNode(.{ .binary = .{ .op = .gte, .left = operand, .right = high } }), .right = try self.scalarNode(.{ .binary = .{ .op = .lte, .left = operand, .right = low } }) } });
                        left = try self.scalarNode(.{ .binary = .{ .op = .@"or", .left = left, .right = reversed } });
                    }
                    if (negated) left = try self.scalarNode(.{ .unary = .{ .op = .not, .operand = left } });
                    continue;
                }
                if (self.keyword(.in)) {
                    try self.expect(.lparen);
                    if (self.pos < self.tokens.len and (self.tokens[self.pos].isKeyword(.select) or self.tokens[self.pos].isKeyword(.with))) {
                        if (self.relation_depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL subquery nesting limit exceeded");
                        self.relation_depth += 1;
                        const query = try self.alloc.create(ast.Select);
                        const nested = try self.statement();
                        if (nested != .select) return self.fail(error.InvalidSqlSyntax, "subquery requires SELECT");
                        query.* = nested.select;
                        const tuple = left.* == .call and std.mem.eql(u8, left.call.name, "$row");
                        if (!tuple) try self.singleColumnSubquery(query.*);
                        self.relation_depth -= 1;
                        try self.expect(.rparen);
                        left = try self.scalarNode(.{ .call = .{ .name = "$in_subquery", .args = try self.alloc.dupe(*const ast.Scalar, &.{left}), .subquery = query } });
                        if (negated) left = try self.scalarNode(.{ .unary = .{ .op = .not, .operand = left } });
                        continue;
                    }
                    var values: std.ArrayList(*const ast.Scalar) = .empty;
                    while (true) {
                        try values.append(self.alloc, try self.scalar(depth + 1, 0));
                        if (!self.take(.comma)) break;
                    }
                    try self.expect(.rparen);
                    left = try self.scalarNode(.{ .in_list = .{ .operand = left, .values = try values.toOwnedSlice(self.alloc), .negated = negated } });
                    continue;
                }
                if (negated) {
                    const insensitive = self.keyword(.ilike);
                    if (!insensitive) try self.expectKeyword(.like);
                    const every = self.keyword(.all);
                    if (every or self.keyword(.any) or self.keyword(.some)) {
                        try self.expect(.lparen);
                        if (self.pos < self.tokens.len and !self.tokens[self.pos].isKeyword(.select) and !self.tokens[self.pos].isKeyword(.with)) {
                            const input = try self.scalar(depth + 1, 0);
                            try self.expect(.rparen);
                            left = try self.patternArray(left, input, every, insensitive, true);
                            continue;
                        }
                        if (self.relation_depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL subquery nesting limit exceeded");
                        self.relation_depth += 1;
                        const query = try self.alloc.create(ast.Select);
                        const nested = try self.statement();
                        if (nested != .select) return self.fail(error.InvalidSqlSyntax, "subquery requires SELECT");
                        query.* = nested.select;
                        try self.singleColumnSubquery(query.*);
                        self.relation_depth -= 1;
                        try self.expect(.rparen);
                        left = try self.scalarNode(.{ .call = .{
                            .name = if (every) (if (insensitive) "$all_not_ilike" else "$all_not_like") else (if (insensitive) "$any_not_ilike" else "$any_not_like"),
                            .args = try self.alloc.dupe(*const ast.Scalar, &.{left}),
                            .subquery = query,
                        } });
                        continue;
                    }
                    const pattern = try self.scalar(depth + 1, 4);
                    left = try self.scalarNode(.{ .unary = .{ .op = .not, .operand = try self.likeExpression(left, pattern, insensitive, depth) } });
                    continue;
                }
            }
            if (self.peek(.colon_colon)) {
                if (minimum > 8) break;
                self.pos += 1;
                const kind = try self.castType();
                left = try self.scalarNode(.{ .cast = .{ .operand = left, .type = kind.type, .element_type = kind.element_type, .numeric_modifier = kind.numeric_modifier, .temporal_kind = kind.temporal_kind } });
                continue;
            }
            // PostgreSQL's postfix spellings share the ordinary IS NULL
            // precedence. Quoted identifiers are never consumed as operators.
            if (minimum <= 3 and self.peek(.identifier) and !self.tokens[self.pos].owned) {
                const spelling = self.tokens[self.pos].text;
                if (std.ascii.eqlIgnoreCase(spelling, "isnull") or std.ascii.eqlIgnoreCase(spelling, "notnull")) {
                    self.pos += 1;
                    left = try self.scalarNode(.{ .unary = .{ .op = if (std.ascii.eqlIgnoreCase(spelling, "isnull")) .is_null else .is_not_null, .operand = left } });
                    continue;
                }
            }
            if (minimum <= 3 and self.keyword(.is)) {
                const negated = self.keyword(.not);
                if (self.keyword(.distinct)) {
                    try self.expectKeyword(.from);
                    left = try self.scalarNode(.{ .binary = .{ .op = if (negated) .is_not_distinct else .is_distinct, .left = left, .right = try self.scalar(depth + 1, 4) } });
                } else {
                    const op: ast.Scalar.Unary = if (self.keyword(.null)) (if (negated) .is_not_null else .is_null) else if (self.keyword(.true)) (if (negated) .is_not_true else .is_true) else if (self.keyword(.false)) (if (negated) .is_not_false else .is_false) else if (self.ddlWord("unknown")) (if (negated) .is_not_unknown else .is_unknown) else return self.fail(error.InvalidSqlSyntax, "expected NULL, TRUE, FALSE, UNKNOWN or DISTINCT");
                    left = try self.scalarNode(.{ .unary = .{ .op = op, .operand = left } });
                }
                continue;
            }
            const current = self.tokens[self.pos];
            if (minimum <= 4 and (current.kind == .regex_match or current.kind == .regex_imatch or current.kind == .regex_not_match or current.kind == .regex_not_imatch)) {
                self.pos += 1;
                const right = try self.scalar(depth + 1, 5);
                const insensitive = current.kind == .regex_imatch or current.kind == .regex_not_imatch;
                const flags = try self.scalarNode(.{ .literal = .{ .string = if (insensitive) "i" else "c" } });
                const matched = try self.scalarNode(.{ .call = .{ .name = "$regex_operator", .args = try self.alloc.dupe(*const ast.Scalar, &.{ left, right, flags }) } });
                // Ordinary PostgreSQL regex operators share the prepared ARE
                // session and admission of regexp_like, rather than a second
                // regex implementation or per-row parser. NOT preserves NULL.
                left = if (current.kind == .regex_not_match or current.kind == .regex_not_imatch)
                    try self.scalarNode(.{ .unary = .{ .op = .not, .operand = matched } })
                else
                    matched;
                continue;
            }
            if ((current.kind == .at_contains or current.kind == .range_overlap or current.kind == .question or current.kind == .question_any or current.kind == .question_all) and minimum <= 4) {
                self.pos += 1;
                const right = try self.scalar(depth + 1, 5);
                left = try self.scalarNode(.{ .call = .{ .name = switch (current.kind) {
                    .at_contains => "$contains",
                    .range_overlap => "$overlaps",
                    .question_any => "jsonb_exists_any",
                    .question_all => "jsonb_exists_all",
                    else => "jsonb_exists",
                }, .args = try self.alloc.dupe(*const ast.Scalar, &.{ left, right }) } });
                continue;
            }
            if ((current.kind == .path_arrow_json or current.kind == .path_arrow_text) and minimum <= 4) {
                self.pos += 1;
                var path = try self.scalar(depth + 1, 5);
                if (path.* == .literal and (path.literal == .string or path.literal == .null))
                    path = try self.scalarNode(.{ .cast = .{ .operand = path, .type = .array, .element_type = .text } });
                left = try self.scalarNode(.{ .call = .{
                    .name = if (current.kind == .path_arrow_json) "$json_path" else "$json_path_text",
                    .args = try self.alloc.dupe(*const ast.Scalar, &.{ left, path }),
                } });
                continue;
            }
            const op: ast.Scalar.Binary = switch (current.kind) {
                .plus => .add,
                .minus => .subtract,
                .star => .multiply,
                .slash => .divide,
                .percent => .modulo,
                .pipe_concat => .concat,
                .arrow_json => .json_get,
                .arrow_text => .json_text,
                .eq => .eq,
                .neq => .neq,
                .lt => .lt,
                .lte => .lte,
                .gt => .gt,
                .gte => .gte,
                else => if (current.isKeyword(.@"and")) .@"and" else if (current.isKeyword(.@"or")) .@"or" else if (current.isKeyword(.like)) .like else if (current.isKeyword(.ilike)) .ilike else break,
            };
            const precedence: u8 = switch (op) {
                .@"or" => 1,
                .@"and" => 2,
                .eq, .neq, .lt, .lte, .gt, .gte, .like, .ilike, .is_distinct, .is_not_distinct => 3,
                .concat => 4,
                .add, .subtract => 5,
                .multiply, .divide, .modulo => 6,
                .json_get, .json_text => 4,
            };
            if (precedence < minimum) break;
            self.pos += 1;
            if (op == .eq or op == .neq or op == .lt or op == .lte or op == .gt or op == .gte or op == .like or op == .ilike) {
                const every = self.keyword(.all);
                if (every or self.keyword(.any) or self.keyword(.some)) {
                    try self.expect(.lparen);
                    if (self.pos < self.tokens.len and !self.tokens[self.pos].isKeyword(.select) and !self.tokens[self.pos].isKeyword(.with)) {
                        const input = try self.scalar(depth + 1, 0);
                        try self.expect(.rparen);
                        if (op == .like or op == .ilike) {
                            left = try self.patternArray(left, input, every, op == .ilike, false);
                            continue;
                        }
                        const comparison_value: @import("array_value.zig").Comparison = switch (op) {
                            .eq => .eq,
                            .neq => .ne,
                            .lt => .lt,
                            .lte => .le,
                            .gt => .gt,
                            .gte => .ge,
                            else => unreachable,
                        };
                        const comparison_node = try self.scalarNode(.{ .literal = .{ .integer = @backingInt(comparison_value) } });
                        const all_node = try self.scalarNode(.{ .literal = .{ .boolean = every } });
                        left = try self.scalarNode(.{ .call = .{ .name = "$array_quantified", .args = try self.alloc.dupe(*const ast.Scalar, &.{ left, input, comparison_node, all_node }) } });
                        continue;
                    }
                    if (self.relation_depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL subquery nesting limit exceeded");
                    self.relation_depth += 1;
                    const query = try self.alloc.create(ast.Select);
                    const nested = try self.statement();
                    if (nested != .select) return self.fail(error.InvalidSqlSyntax, "subquery requires SELECT");
                    query.* = nested.select;
                    try self.singleColumnSubquery(query.*);
                    self.relation_depth -= 1;
                    try self.expect(.rparen);
                    left = try self.scalarNode(.{ .call = .{
                        .name = try std.fmt.allocPrint(self.alloc, "${s}_{s}", .{ if (every) "all" else "any", @tagName(op) }),
                        .args = try self.alloc.dupe(*const ast.Scalar, &.{left}),
                        .subquery = query,
                    } });
                    continue;
                }
            }
            const right = try self.scalar(depth + 1, precedence + 1);
            left = if (op == .like or op == .ilike) try self.likeExpression(left, right, op == .ilike, depth) else try self.scalarNode(.{ .binary = .{ .op = op, .left = left, .right = right } });
        }
        return left;
    }

    fn likeExpression(self: *Parser, operand: *const ast.Scalar, pattern: *const ast.Scalar, insensitive: bool, depth: usize) Error!*const ast.Scalar {
        if (!self.keyword(.escape)) return self.scalarNode(.{ .binary = .{ .op = if (insensitive) .ilike else .like, .left = operand, .right = pattern } });
        const escape = try self.scalar(depth + 1, 4);
        const mode = try self.scalarNode(.{ .literal = .{ .boolean = insensitive } });
        return self.scalarNode(.{ .call = .{ .name = "$like_escape", .args = try self.alloc.dupe(*const ast.Scalar, &.{ operand, pattern, escape, mode }) } });
    }

    /// Bare nested brackets are array grammar, not a new scalar expression.
    fn arrayConstructor(self: *Parser, depth: usize) Error!*const ast.Scalar {
        if (depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL array nesting budget exceeded");
        try self.expect(.lbracket);
        var elements: std.ArrayList(*const ast.Scalar) = .empty;
        if (!self.take(.rbracket)) {
            const nested_brackets = self.peek(.lbracket);
            while (true) {
                if (self.peek(.lbracket) != nested_brackets) return self.fail(error.InvalidSqlSyntax, "nested array bracket lists cannot mix with scalar expressions");
                const element = if (nested_brackets) try self.arrayConstructor(depth + 1) else try self.scalar(depth + 1, 0);
                try elements.append(self.alloc, element);
                if (!self.take(.comma)) break;
            }
            try self.expect(.rbracket);
        }
        return self.scalarNode(.{ .call = .{ .name = "$array", .args = try elements.toOwnedSlice(self.alloc) } });
    }

    fn checkScalarDepth(self: *Parser, expression: *const ast.Scalar, depth: usize) Error!void {
        if (depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL scalar tree depth budget exceeded");
        switch (expression.*) {
            .unary => |part| try self.checkScalarDepth(part.operand, depth + 1),
            .binary => |part| {
                try self.checkScalarDepth(part.left, depth + 1);
                try self.checkScalarDepth(part.right, depth + 1);
            },
            .cast => |part| try self.checkScalarDepth(part.operand, depth + 1),
            .call => |part| {
                for (part.args) |arg| try self.checkScalarDepth(arg, depth + 1);
                if (part.filter) |filter| try self.checkScalarDepth(filter, depth + 1);
                if (part.window) |spec| {
                    for (spec.partition) |item| try self.checkScalarDepth(item, depth + 1);
                    for (spec.order) |item| if (item.expression) |expression_| try self.checkScalarDepth(expression_, depth + 1);
                }
            },
            .case_when => |part| {
                for (part.branches) |branch| {
                    try self.checkScalarDepth(branch.condition, depth + 1);
                    try self.checkScalarDepth(branch.value, depth + 1);
                }
                if (part.otherwise) |other| try self.checkScalarDepth(other, depth + 1);
            },
            .in_list => |part| {
                try self.checkScalarDepth(part.operand, depth + 1);
                for (part.values) |item| try self.checkScalarDepth(item, depth + 1);
            },
            .literal, .column => {},
        }
    }

    fn predicateNode(self: *Parser, expression: ast.Predicate) Error!*const ast.Predicate {
        try self.node();
        const result = try self.alloc.create(ast.Predicate);
        result.* = expression;
        return result;
    }

    fn predicate(self: *Parser, depth: usize, minimum: u8) Error!*const ast.Predicate {
        if (depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL expression nesting budget exceeded");
        if (minimum <= 2) {
            const conjunction = minimum == 2;
            const next: u8 = if (conjunction) 3 else 2;
            const first = try self.predicate(depth, next);
            const operator: token.TokenKeyword = if (conjunction) .@"and" else .@"or";
            if (!self.keyword(operator)) return first;
            // Preserve precedence while balancing associative boolean chains.
            // This keeps downstream binding/evaluation logarithmic in stack
            // depth even for thousands of AND/OR terms.
            var operands = std.ArrayList(*const ast.Predicate).empty;
            try operands.append(self.alloc, first);
            while (true) {
                try operands.append(self.alloc, try self.predicate(depth, next));
                if (!self.keyword(operator)) break;
            }
            return self.balancedPredicate(operands.items, conjunction);
        }
        var left: *const ast.Predicate = undefined;
        if (self.keyword(.not)) {
            left = try self.predicateNode(.{ .negation = try self.predicate(depth + 1, 3) });
        } else if (self.take(.lparen)) {
            left = try self.predicate(depth + 1, 0);
            try self.expect(.rparen);
        } else {
            const column = try self.field();
            if (self.keyword(.is)) {
                const negated = self.keyword(.not);
                try self.expectKeyword(.null);
                left = try self.predicateNode(.{ .is_null = .{ .field = column, .negated = negated } });
            } else {
                if (self.pos >= self.tokens.len) return self.fail(error.InvalidSqlSyntax, "expected comparison operator");
                const op: ast.Comparison = switch (self.tokens[self.pos].kind) {
                    .eq => .eq,
                    .neq => .neq,
                    .lt => .lt,
                    .lte => .lte,
                    .gt => .gt,
                    .gte => .gte,
                    else => return self.fail(error.UnsupportedSqlShape, "unsupported SQL predicate operator"),
                };
                self.pos += 1;
                left = try self.predicateNode(.{ .comparison = .{ .field = column, .op = op, .value = try self.value() } });
            }
        }
        return left;
    }

    fn balancedPredicate(self: *Parser, operands: []const *const ast.Predicate, conjunction: bool) Error!*const ast.Predicate {
        if (operands.len == 1) return operands[0];
        const middle = operands.len / 2;
        const left = try self.balancedPredicate(operands[0..middle], conjunction);
        const right = try self.balancedPredicate(operands[middle..], conjunction);
        return self.predicateNode(if (conjunction) .{ .conjunction = .{ .left = left, .right = right } } else .{ .disjunction = .{ .left = left, .right = right } });
    }

    fn checkPredicateDepth(self: *Parser, expression: *const ast.Predicate, depth: usize) Error!void {
        if (depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL expression tree depth budget exceeded");
        switch (expression.*) {
            .conjunction, .disjunction => |binary| {
                try self.checkPredicateDepth(binary.left, depth + 1);
                try self.checkPredicateDepth(binary.right, depth + 1);
            },
            .negation => |operand| try self.checkPredicateDepth(operand, depth + 1),
            .scalar => |expression_node| try self.checkScalarDepth(expression_node, depth + 1),
            .comparison, .is_null => {},
        }
    }

    fn where(self: *Parser) Error!?*const ast.Predicate {
        if (!self.keyword(.where)) return null;
        const start = self.pos;
        const result = self.predicate(0, 0) catch |err| switch (err) {
            error.InvalidSqlSyntax, error.UnsupportedSqlShape => {
                self.pos = start;
                const expression = try self.scalar(0, 0);
                try self.checkScalarDepth(expression, 0);
                return self.predicateNode(.{ .scalar = expression });
            },
            else => return err,
        };
        if (self.pos < self.tokens.len and self.tokens[self.pos].kind != .semicolon and !self.tokens[self.pos].isKeyword(.order) and !self.tokens[self.pos].isKeyword(.limit) and !self.tokens[self.pos].isKeyword(.offset) and !self.tokens[self.pos].isKeyword(.fetch) and !self.tokens[self.pos].isKeyword(.returning)) {
            self.pos = start;
            const expression = try self.scalar(0, 0);
            try self.checkScalarDepth(expression, 0);
            return self.predicateNode(.{ .scalar = expression });
        }
        try self.checkPredicateDepth(result, 0);
        return result;
    }

    fn select(self: *Parser) Error!ast.Select {
        return self.selectFinish(try self.selectCore());
    }

    // VALUES shares the flat, bounded row-source representation used by
    // INSERT sources; it is not a chain of pairwise UNION coercions.
    fn valuesCore(self: *Parser) Error!ast.Select {
        var arms: std.ArrayList(*const ast.Select) = .empty;
        var width: ?usize = null;
        while (true) {
            if (arms.items.len >= self.limits.max_insert_rows) return self.fail(error.SqlLimitExceeded, "VALUES row budget exceeded");
            try self.expect(.lparen);
            var projections: std.ArrayList(ast.Projection) = .empty;
            while (true) {
                try self.node();
                const expression = try self.scalar(0, 0);
                try self.checkScalarDepth(expression, 0);
                try projections.append(self.alloc, .{ .alias = try std.fmt.allocPrint(self.alloc, "column{d}", .{projections.items.len + 1}), .expression = expression });
                if (!self.take(.comma)) break;
            }
            try self.expect(.rparen);
            if (width) |expected| {
                if (projections.items.len != expected) return self.fail(error.InvalidSqlSyntax, "VALUES rows must have equal width");
            } else width = projections.items.len;
            const leaf = try self.alloc.create(ast.Select);
            leaf.* = .{ .columns = try projections.toOwnedSlice(self.alloc), .generated_values = true };
            try arms.append(self.alloc, leaf);
            if (!self.take(.comma)) break;
        }
        return .{ .values_arms = try arms.toOwnedSlice(self.alloc), .generated_values = true };
    }

    fn selectFinish(self: *Parser, first: ast.Select) Error!ast.Select {
        var result = try self.setTail(first, 1, 0);
        result.order_by = try self.selectOrder();
        try @import("window_names.zig").resolveOrder(self.alloc, result, self.limits.max_depth);
        // PostgreSQL admits either pagination-clause order. Normalize FETCH
        // into the same bound as LIMIT, retaining explicit NULL/ALL as an
        // unbounded request rather than a configured-result truncation.
        var has_limit = false;
        var has_offset = false;
        while (true) {
            if (self.keyword(.limit)) {
                if (has_limit) return self.fail(error.InvalidSqlSyntax, "duplicate row limit");
                has_limit = true;
                result.limit = if (self.keyword(.all)) .null else try self.rowBound();
            } else if (self.keyword(.offset)) {
                if (has_offset) return self.fail(error.InvalidSqlSyntax, "duplicate row offset");
                has_offset = true;
                result.offset = try self.rowBound();
                if (!self.keyword(.row)) _ = self.keyword(.rows);
            } else if (self.keyword(.fetch)) {
                if (has_limit) return self.fail(error.InvalidSqlSyntax, "duplicate row limit");
                has_limit = true;
                if (!self.keyword(.first)) try self.expectKeyword(.next);
                result.limit = if (self.pos < self.tokens.len and (self.tokens[self.pos].isKeyword(.row) or self.tokens[self.pos].isKeyword(.rows))) .{ .integer = 1 } else try self.rowBound();
                if (!self.keyword(.row)) try self.expectKeyword(.rows);
                if (self.keyword(.with)) return self.fail(error.UnsupportedSqlShape, "FETCH WITH TIES requires peer-aware result admission");
                try self.expectKeyword(.only);
            } else break;
        }
        if (result.distinct == null and result.columns.len == 1 and result.group_by.len == 0 and result.having == null and result.order_by.len == 0) {
            if (result.columns[0].expression) |expression| if (expression.* == .call and expression.call.window == null and expression.call.within_group == null and expression.call.star and !expression.call.distinct and expression.call.filter == null and std.mem.eql(u8, expression.call.name, "count")) {
                result.count_all = true;
                result.count_alias = result.columns[0].alias;
                result.columns = &.{};
            };
        }
        return result;
    }

    fn setTail(self: *Parser, first: ast.Select, minimum: u8, depth: usize) Error!ast.Select {
        if (depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL set nesting limit exceeded");
        var left = first;
        while (self.pos < self.tokens.len) {
            const next_token = self.tokens[self.pos];
            const kind: ast.SetKind = if (next_token.isKeyword(.@"union")) .@"union" else if (next_token.isKeyword(.intersect)) .intersect else if (next_token.isKeyword(.except)) .except else break;
            const precedence: u8 = if (kind == .intersect) 2 else 1;
            if (precedence < minimum) break;
            self.pos += 1;
            const all = self.keyword(.all);
            if (!all) _ = self.keyword(.distinct);
            var right: ast.Select = undefined;
            if (self.take(.lparen)) {
                self.relation_depth += 1;
                defer self.relation_depth -= 1;
                if (self.relation_depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL set nesting limit exceeded");
                const statement_value = try self.statement();
                if (statement_value != .select) return self.fail(error.InvalidSqlSyntax, "set operands require SELECT");
                right = statement_value.select;
                try self.expect(.rparen);
            } else {
                if (self.keyword(.values)) {
                    right = try self.valuesCore();
                } else {
                    try self.expectKeyword(.select);
                    right = try self.selectCore();
                }
            }
            right = try self.setTail(right, precedence + 1, depth + 1);
            try self.node();
            const left_ptr = try self.alloc.create(ast.Select);
            left_ptr.* = left;
            const right_ptr = try self.alloc.create(ast.Select);
            right_ptr.* = right;
            left = .{ .set_operation = .{ .kind = kind, .all = all, .left = left_ptr, .right = right_ptr } };
        }
        return left;
    }

    fn selectCore(self: *Parser) Error!ast.Select {
        const distinct: ?[]const *const ast.Scalar = if (self.keyword(.distinct)) blk: {
            if (!self.keyword(.on)) break :blk &.{};
            try self.expect(.lparen);
            var keys: std.ArrayList(*const ast.Scalar) = .empty;
            while (true) {
                try keys.append(self.alloc, try self.scalar(0, 0));
                if (!self.take(.comma)) break;
            }
            try self.expect(.rparen);
            break :blk try keys.toOwnedSlice(self.alloc);
        } else blk: {
            _ = self.keyword(.all);
            break :blk null;
        };
        var columns = std.ArrayList(ast.Projection).empty;
        while (true) {
            try columns.append(self.alloc, try self.parseProjection(true));
            if (!self.take(.comma)) break;
        }
        const source = if (self.keyword(.from)) try self.relation() else null;
        if (source == null) for (columns.items) |projection| if (projection.wildcard and projection.field.len == 0) return self.fail(error.InvalidSqlSyntax, "wildcard projection requires a FROM source");
        // Preserve the established compact representation of a lone * only
        // after validating its source; an empty projection loses that marker.
        if (columns.items.len == 1 and columns.items[0].wildcard and columns.items[0].field.len == 0) columns.clearRetainingCapacity();
        const simple = source != null and source.?.* == .table and source.?.table.alias == null and source.?.table.search == null;
        const table = if (simple) source.?.table.name else null;
        const filter = try self.where();
        var group_by: std.ArrayList(*const ast.Scalar) = .empty;
        if (self.keyword(.group)) {
            try self.expectKeyword(.by);
            while (true) {
                try group_by.append(self.alloc, try self.scalar(0, 0));
                if (!self.take(.comma)) break;
            }
        }
        const having = if (self.keyword(.having)) try self.scalar(0, 0) else null;
        var windows: std.ArrayList(ast.NamedWindow) = .empty;
        if (self.keyword(.window)) while (true) {
            try self.node();
            const window_name = try self.identifier();
            try self.expectKeyword(.as);
            if (!self.peek(.lparen)) return self.fail(error.InvalidSqlSyntax, "window definition requires parentheses");
            try windows.append(self.alloc, .{ .name = window_name, .window = try self.window(0) });
            if (!self.take(.comma)) break;
        };
        var result = ast.Select{ .distinct = distinct, .table = table, .source = if (simple) null else source, .columns = try columns.toOwnedSlice(self.alloc), .predicate = filter, .group_by = try group_by.toOwnedSlice(self.alloc), .having = having, .windows = try windows.toOwnedSlice(self.alloc) };
        try @import("window_names.zig").resolveCore(self.alloc, &result, self.limits.max_depth);
        return result;
    }

    fn window(self: *Parser, depth: usize) Error!ast.Window {
        if (!self.peek(.lparen)) return .{ .reference = try self.identifier() };
        try self.expect(.lparen);
        const reference = if (self.peek(.identifier) and !self.tokens[self.pos].isKeyword(.partition) and !self.tokens[self.pos].isKeyword(.order) and !self.tokens[self.pos].isKeyword(.rows) and !self.tokens[self.pos].isKeyword(.range) and (self.tokens[self.pos].owned or !std.ascii.eqlIgnoreCase(self.tokens[self.pos].text, "groups"))) try self.identifier() else null;
        var partitions: std.ArrayList(*const ast.Scalar) = .empty;
        if (self.keyword(.partition)) {
            try self.expectKeyword(.by);
            while (true) {
                try partitions.append(self.alloc, try self.scalar(depth, 0));
                if (!self.take(.comma)) break;
            }
        }
        const orders = try self.orderExpressions(false);
        var frame: ?ast.Window.Frame = null;
        const mode: ?@FieldType(ast.Window.Frame, "mode") = if (self.keyword(.rows)) .rows else if (self.keyword(.range)) .range else if (self.ddlWord("groups")) .groups else null;
        if (mode) |kind| {
            const between = self.keyword(.between);
            const first = try self.windowBound();
            const last = if (between) blk: {
                try self.expectKeyword(.@"and");
                break :blk try self.windowBound();
            } else ast.Window.Bound.current;
            if (first == .unbounded_following or last == .unbounded_preceding) return self.fail(error.InvalidSqlSyntax, "invalid window frame boundary");
            if ((first == .following and (last == .current or last == .preceding)) or (first == .current and last == .preceding)) return self.fail(error.InvalidSqlSyntax, "window frame end cannot precede its start category");
            frame = .{ .mode = kind, .start = first, .end = last };
            if (self.ddlWord("exclude")) {
                frame.?.exclusion = if (self.keyword(.current)) blk: {
                    try self.expectKeyword(.row);
                    break :blk .current;
                } else if (self.keyword(.group)) .group else if (self.ddlWord("ties")) .ties else blk: {
                    if (!self.ddlWord("no") or !self.ddlWord("others")) return self.fail(error.InvalidSqlSyntax, "invalid window exclusion");
                    break :blk .no_others;
                };
            }
        }
        try self.expect(.rparen);
        return .{ .reference = reference, .copy_reference = reference != null, .partition = try partitions.toOwnedSlice(self.alloc), .order = orders, .frame = frame };
    }

    fn windowBound(self: *Parser) Error!ast.Window.Bound {
        if (self.keyword(.unbounded)) {
            if (self.keyword(.preceding)) return .unbounded_preceding;
            try self.expectKeyword(.following);
            return .unbounded_following;
        }
        if (self.keyword(.current)) {
            try self.expectKeyword(.row);
            return .current;
        }
        const offset = try self.value();
        if (offset != .parameter and (offset != .integer or offset.integer < 0)) return self.fail(error.InvalidSqlSyntax, "window frame offset must be a nonnegative integer or parameter");
        if (self.keyword(.preceding)) return .{ .preceding = offset };
        try self.expectKeyword(.following);
        return .{ .following = offset };
    }

    fn selectOrder(self: *Parser) Error![]const ast.Order {
        return self.orderExpressions(true);
    }

    fn orderExpressions(self: *Parser, positions: bool) Error![]const ast.Order {
        return self.orderExpressionsDepth(positions, 0);
    }
    fn orderExpressionsDepth(self: *Parser, positions: bool, depth: usize) Error![]const ast.Order {
        var order_by = std.ArrayList(ast.Order).empty;
        if (self.keyword(.order)) {
            try self.expectKeyword(.by);
            while (true) {
                try self.node();
                const expression = try self.scalar(depth, 0);
                try self.checkScalarDepth(expression, 0);
                var descending = self.keyword(.desc);
                const explicit_direction = descending or self.keyword(.asc);
                if (!explicit_direction and self.keyword(.using)) {
                    if (self.take(.gt)) descending = true else if (!self.take(.lt)) return self.fail(error.UnsupportedSqlShape, "ORDER BY USING requires a supported ordering operator (< or >)");
                }
                var order: ast.Order = .{ .descending = descending };
                if (expression.* == .column) order.field = expression.column else if (positions and expression.* == .literal and expression.literal == .integer) {
                    order.position = std.math.cast(u32, expression.literal.integer) orelse return self.fail(error.InvalidSqlSyntax, "ORDER BY position must be a positive integer");
                    if (order.position.? == 0) return self.fail(error.InvalidSqlSyntax, "ORDER BY position must be a positive integer");
                } else order.expression = expression;
                if (self.keyword(.nulls)) {
                    if (self.keyword(.first)) order.nulls_first = true else {
                        try self.expectKeyword(.last);
                        order.nulls_first = false;
                    }
                }
                try order_by.append(self.alloc, order);
                if (!self.take(.comma)) break;
            }
        }
        return order_by.toOwnedSlice(self.alloc);
    }

    fn relationNode(self: *Parser, relation_value: ast.Relation) Error!*const ast.Relation {
        try self.node();
        const result = try self.alloc.create(ast.Relation);
        result.* = relation_value;
        return result;
    }

    fn sourceAlias(self: *Parser) Error!?[]const u8 {
        if (self.keyword(.as)) return try self.identifier();
        if (self.peek(.identifier) and (self.tokens[self.pos].keyword == null or self.tokens[self.pos].owned)) return try self.identifier();
        return null;
    }

    fn relationAtom(self: *Parser) Error!*const ast.Relation {
        const lateral = self.keyword(.lateral);
        if (self.take(.lparen)) {
            self.relation_depth += 1;
            defer self.relation_depth -= 1;
            if (self.relation_depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL relation nesting limit exceeded");
            const statement_value = try self.statement();
            if (statement_value != .select) return self.fail(error.InvalidSqlSyntax, "derived relation requires SELECT");
            try self.expect(.rparen);
            const alias = try self.sourceAlias() orelse return self.fail(error.InvalidSqlSyntax, "derived relation requires an alias");
            var names: std.ArrayList([]const u8) = .empty;
            if (self.take(.lparen)) {
                while (true) {
                    try self.node();
                    try names.append(self.alloc, try self.identifier());
                    if (!self.take(.comma)) break;
                }
                try self.expect(.rparen);
            }
            const query = try self.alloc.create(ast.Select);
            query.* = statement_value.select;
            return self.relationNode(.{ .derived = .{ .query = query, .alias = alias, .columns = try names.toOwnedSlice(self.alloc), .lateral = lateral } });
        }
        const name_value = try self.tableReferenceName();
        if (name_value.database == null and (name_value.namespace == null or std.mem.eql(u8, name_value.namespace.?, "pg_catalog")) and std.mem.eql(u8, name_value.table, "jsonb_to_record") and self.take(.lparen)) {
            var input = try self.scalar(0, 0);
            try self.expect(.rparen);
            // Only unknown literals acquire the function's JSONB input type.
            // A typed TEXT/integer argument must not become an implicit cast.
            if (input.* == .literal and input.literal == .string)
                input = try self.scalarNode(.{ .cast = .{ .operand = input, .type = .json, .element_type = .jsonb } });
            const alias = try self.sourceAlias() orelse "jsonb_to_record";
            try self.expect(.lparen);
            var output: std.ArrayList(ast.Projection) = .empty;
            const document = try self.scalarNode(.{ .column = "$record_input\x00$document" });
            while (true) {
                if (output.items.len == 1024) return self.fail(error.SqlLimitExceeded, "JSON record has too many columns");
                const column = try self.identifier();
                for (output.items) |prior| if (std.mem.eql(u8, prior.alias.?, column)) return self.fail(error.DuplicateSqlColumn, "JSON record column names must be unique");
                const kind = try self.castType();
                const hint = try self.scalarNode(.{ .cast = .{ .operand = try self.scalarNode(.{ .literal = .null }), .type = kind.type, .element_type = kind.element_type, .numeric_modifier = kind.numeric_modifier, .temporal_kind = kind.temporal_kind } });
                const args = try self.alloc.dupe(*const ast.Scalar, &.{ document, try self.scalarNode(.{ .literal = .{ .string = column } }), hint });
                try output.append(self.alloc, .{ .alias = column, .expression = try self.scalarNode(.{ .call = .{ .name = "$record_field", .args = args } }) });
                if (!self.take(.comma)) break;
            }
            try self.expect(.rparen);
            const producer = try self.alloc.create(ast.Select);
            producer.* = .{ .columns = try self.alloc.dupe(ast.Projection, &.{.{ .alias = "$document", .expression = input }}) };
            const query = try self.alloc.create(ast.Select);
            const valid = try self.scalarNode(.{ .call = .{ .name = "$record_validate", .args = try self.alloc.dupe(*const ast.Scalar, &.{document}) } });
            const validation = try self.alloc.create(ast.Predicate);
            validation.* = .{ .scalar = valid };
            query.* = .{
                .source = try self.relationNode(.{ .derived = .{ .query = producer, .alias = "$record_input", .lateral = true } }),
                .columns = try output.toOwnedSlice(self.alloc),
                // Validate even when the consumer only counts rows. This is
                // a table function, not a discardable SELECT-list cast.
                .predicate = validation,
            };
            // A table function constructs every declared field, including
            // invalid fields discarded by COUNT(*) or an outer projection.
            // Reuse the complete-row materialization boundary so conversion
            // happens once, not once for validation and again for projection.
            const materialized = try self.alloc.create(ast.Select);
            materialized.* = .{
                .source = try self.relationNode(.{ .table = .{ .name = .{ .table = "$record_output\x00" } } }),
                .ctes = try self.alloc.dupe(ast.Cte, &.{.{
                    .name = "$record_output\x00",
                    .query = query,
                    .materialization = .materialized,
                }}),
            };
            return self.relationNode(.{ .derived = .{ .query = materialized, .alias = alias, .lateral = true } });
        }
        if (name_value.database == null and (name_value.namespace == null or std.mem.eql(u8, name_value.namespace.?, "pg_catalog")) and std.mem.eql(u8, name_value.table, "unnest") and self.take(.lparen)) {
            var args: std.ArrayList(*const ast.Scalar) = .empty;
            while (true) {
                try args.append(self.alloc, try self.scalar(0, 0));
                if (!self.take(.comma)) break;
            }
            try self.expect(.rparen);
            const ordinality = if (self.keyword(.with)) blk: {
                if (!self.ddlWord("ordinality")) return self.fail(error.InvalidSqlSyntax, "expected WITH ORDINALITY");
                break :blk true;
            } else false;
            const alias = try self.sourceAlias();
            var columns: std.ArrayList([]const u8) = .empty;
            if (alias != null and self.take(.lparen)) {
                while (true) {
                    try columns.append(self.alloc, try self.identifier());
                    if (!self.take(.comma)) break;
                }
                try self.expect(.rparen);
            }
            return self.relationNode(.{ .unnest = .{ .args = try args.toOwnedSlice(self.alloc), .alias = alias, .columns = try columns.toOwnedSlice(self.alloc), .ordinality = ordinality, .qualified = name_value.namespace != null } });
        }
        if (lateral) return self.fail(error.InvalidSqlSyntax, "LATERAL requires a derived query or table function");
        if (name_value.database == null and name_value.namespace == null and std.ascii.eqlIgnoreCase(name_value.table, "antfly_search") and self.take(.lparen)) {
            const table_value = try self.value();
            if (table_value != .string or table_value.string.len == 0) return self.fail(error.InvalidSqlSyntax, "antfly_search requires a literal table name");
            try self.expect(.comma);
            const request = try self.value();
            if (request != .string and request != .parameter) return self.fail(error.InvalidSqlSyntax, "antfly_search requires query text or a text parameter");
            const limit = if (self.take(.comma)) limit: {
                if (self.keyword(.limit)) {
                    try self.expect(.eq);
                    try self.expect(.gt);
                }
                break :limit try self.value();
            } else null;
            try self.expect(.rparen);
            const search = try self.alloc.create(ast.Search);
            search.* = .{ .request = request, .limit = limit };
            return self.relationNode(.{ .table = .{ .name = .{ .table = table_value.string }, .search = search, .alias = (try self.sourceAlias()) orelse "antfly_search" } });
        }
        return self.relationNode(.{ .table = .{ .name = name_value, .alias = try self.sourceAlias() } });
    }

    fn relation(self: *Parser) Error!*const ast.Relation {
        return self.relationTail(try self.relationAtom());
    }

    fn relationTail(self: *Parser, first: *const ast.Relation) Error!*const ast.Relation {
        var left = first;
        var depth: usize = 1;
        while (true) {
            var kind: ast.JoinKind = .inner;
            const natural = self.keyword(.natural);
            if (self.take(.comma)) kind = .cross else if (self.keyword(.join)) {} else {
                if (self.keyword(.inner)) kind = .inner else if (self.keyword(.left)) kind = .left else if (self.keyword(.right)) kind = .right else if (self.keyword(.full)) kind = .full else if (self.keyword(.cross)) kind = .cross else {
                    if (natural) return self.fail(error.InvalidSqlSyntax, "NATURAL requires JOIN");
                    break;
                }
                _ = self.keyword(.outer);
                try self.expectKeyword(.join);
            }
            if (natural and kind == .cross) return self.fail(error.InvalidSqlSyntax, "NATURAL CROSS JOIN is not allowed");
            depth += 1;
            if (depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL join nesting limit exceeded");
            const right = try self.relationAtom();
            var keys: @FieldType(@FieldType(ast.Relation, "join"), "keys") = if (natural) .natural else .none;
            var using_alias: ?[]const u8 = null;
            const condition = if (kind == .cross or natural) null else if (self.keyword(.using)) blk: {
                try self.expect(.lparen);
                var names: std.ArrayList([]const u8) = .empty;
                while (true) {
                    try names.append(self.alloc, try self.identifier());
                    if (!self.take(.comma)) break;
                }
                try self.expect(.rparen);
                keys = .{ .using = try names.toOwnedSlice(self.alloc) };
                if (self.keyword(.as)) using_alias = try self.identifier();
                break :blk null;
            } else blk: {
                try self.expectKeyword(.on);
                break :blk try self.scalar(0, 0);
            };
            left = try self.relationNode(.{ .join = .{ .kind = kind, .left = left, .right = right, .condition = condition, .keys = keys, .using_alias = using_alias } });
        }
        return left;
    }

    fn rowBound(self: *Parser) Error!ast.Value {
        const result = try self.value();
        switch (result) {
            .integer => {},
            .parameter, .null => {},
            else => return self.fail(error.InvalidSqlSyntax, "row bound must be an integer, NULL or parameter"),
        }
        return result;
    }

    fn insert(self: *Parser) Error!ast.Insert {
        try self.expectKeyword(.into);
        const table = try self.tableReferenceName();
        const alias = if (self.keyword(.as)) try self.identifier() else null;
        if (self.keyword(.default)) {
            try self.expectKeyword(.values);
            const rows = try self.alloc.alloc([]const ast.Value, 1);
            rows[0] = &.{};
            return .{ .table = table, .alias = alias, .columns = &.{}, .rows = rows, .conflict = try self.conflict(), .returning = try self.returning() };
        }
        const implicit_columns = !self.take(.lparen);
        var columns = std.ArrayList([]const u8).empty;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        if (!implicit_columns) {
            while (true) {
                try self.node();
                const column = try self.assignmentField();
                const entry = try seen.getOrPut(self.alloc, column);
                if (entry.found_existing) return self.fail(error.DuplicateSqlColumn, "duplicate INSERT column");
                try columns.append(self.alloc, column);
                if (!self.take(.comma)) break;
            }
            try self.expect(.rparen);
        }
        var overriding: @FieldType(ast.Insert, "overriding") = .none;
        if (self.keyword(.overriding)) {
            overriding = if (self.keyword(.system)) .system else if (self.keyword(.user)) .user else return self.fail(error.InvalidSqlSyntax, "expected SYSTEM or USER after OVERRIDING");
            try self.expectKeyword(.value);
        }
        if (self.pos < self.tokens.len and (self.tokens[self.pos].isKeyword(.select) or self.tokens[self.pos].isKeyword(.with))) {
            const source = try self.alloc.create(ast.Select);
            const statement_ = try self.statement();
            if (statement_ != .select) return self.fail(error.InvalidSqlSyntax, "INSERT source must be SELECT");
            source.* = statement_.select;
            var conflict_clause = try self.conflict();
            if (conflict_clause) |*clause| {
                const captures = try self.conflictCaptures(clause);
                if (captures.len != 0) {
                    if (source.columns.len == 0 or source.set_operation != null or source.values_arms.len != 0) return self.fail(error.UnsupportedSqlShape, "conflict assignment subquery requires an explicit INSERT source projection");
                    const projected = try self.alloc.alloc(ast.Projection, source.columns.len + captures.len);
                    @memcpy(projected[0..source.columns.len], source.columns);
                    for (captures, projected[source.columns.len..], 0..) |expression, *projection, ordinal| {
                        projection.* = .{ .alias = try std.fmt.allocPrint(self.alloc, "$conflict_capture_{d}", .{ordinal}), .expression = expression };
                    }
                    source.columns = projected;
                }
            }
            return .{ .implicit_columns = implicit_columns, .overriding = overriding, .table = table, .alias = alias, .columns = try columns.toOwnedSlice(self.alloc), .source = source, .conflict = conflict_clause, .returning = try self.returning() };
        }
        try self.expectKeyword(.values);
        var rows = std.ArrayList([]const ast.Value).empty;
        var expressions = std.ArrayList([]const ?*const ast.Scalar).empty;
        var defaults = std.ArrayList([]const bool).empty;
        while (true) {
            if (rows.items.len >= self.limits.max_insert_rows) return self.fail(error.SqlLimitExceeded, "INSERT row budget exceeded");
            try self.expect(.lparen);
            var row = std.ArrayList(ast.Value).empty;
            var row_expressions = std.ArrayList(?*const ast.Scalar).empty;
            var row_defaults = std.ArrayList(bool).empty;
            while (true) {
                if (self.keyword(.default)) {
                    try self.node();
                    try row.append(self.alloc, .null);
                    try row_expressions.append(self.alloc, null);
                    try row_defaults.append(self.alloc, true);
                } else {
                    const expression = try self.scalar(0, 0);
                    try self.checkScalarDepth(expression, 0);
                    try row.append(self.alloc, if (expression.* == .literal) expression.literal else .null);
                    try row_expressions.append(self.alloc, if (expression.* == .literal) null else expression);
                    try row_defaults.append(self.alloc, false);
                }
                if (!self.take(.comma)) break;
            }
            try self.expect(.rparen);
            if (implicit_columns and rows.items.len == 0) for (0..row.items.len) |ordinal| {
                // These private labels only give lowered VALUES a fixed width.
                // Binding replaces the target names from the schema; source
                // outputs are consumed by ordinal, never by these labels.
                try columns.append(self.alloc, try std.fmt.allocPrint(self.alloc, "$insert_value_{d}", .{ordinal}));
            };
            if (row.items.len != columns.items.len) return self.fail(error.InvalidSqlSyntax, "INSERT values count does not match columns");
            try rows.append(self.alloc, try row.toOwnedSlice(self.alloc));
            try expressions.append(self.alloc, try row_expressions.toOwnedSlice(self.alloc));
            try defaults.append(self.alloc, try row_defaults.toOwnedSlice(self.alloc));
            if (!self.take(.comma)) break;
        }
        const names = try columns.toOwnedSlice(self.alloc);
        const values = try rows.toOwnedSlice(self.alloc);
        const cells = try expressions.toOwnedSlice(self.alloc);
        const default_cells = try defaults.toOwnedSlice(self.alloc);
        var conflict_clause = try self.conflict();
        const returning_columns = try self.returning();
        const captures = if (conflict_clause) |*clause| try self.conflictCaptures(clause) else &.{};
        var contains_subquery = false;
        for (cells) |row| for (row) |cell| if (cell) |expression| {
            contains_subquery = contains_subquery or @import("subquery_lowering.zig").has(expression);
        };
        if (!contains_subquery and captures.len == 0) return .{ .implicit_columns = implicit_columns, .overriding = overriding, .table = table, .alias = alias, .columns = names, .rows = values, .expressions = cells, .defaults = default_cells, .conflict = conflict_clause, .returning = returning_columns };

        // VALUES with scalar subqueries is one source relation, not a collection
        // of independent expression evaluations. Keep source arms flat and in
        // input order; binding infers their shared output types as one unit.
        // INSERT ... SELECT then captures every source before preparing any
        // target image, including when the source reads the target itself.
        var arms: std.ArrayList(*const ast.Select) = .empty;
        for (values, cells) |row, row_cells| {
            const projections = try self.alloc.alloc(ast.Projection, names.len + captures.len);
            for (names, row, row_cells, projections[0..names.len]) |column_name, literal_value, expression, *projection| {
                const scalar_value = expression orelse blk: {
                    const literal = try self.scalarNode(.{ .literal = literal_value });
                    break :blk literal;
                };
                projection.* = .{ .alias = column_name, .expression = scalar_value };
            }
            for (captures, projections[names.len..], 0..) |expression, *projection, ordinal| {
                projection.* = .{ .alias = try std.fmt.allocPrint(self.alloc, "$conflict_capture_{d}", .{ordinal}), .expression = expression };
            }
            const leaf = try self.alloc.create(ast.Select);
            leaf.* = .{ .columns = projections, .generated_values = true };
            try arms.append(self.alloc, leaf);
        }
        const source = try self.alloc.create(ast.Select);
        source.* = .{ .values_arms = try arms.toOwnedSlice(self.alloc), .generated_values = true };
        return .{ .implicit_columns = implicit_columns, .overriding = overriding, .table = table, .alias = alias, .columns = names, .source = source, .values_source_rows = values, .defaults = default_cells, .conflict = conflict_clause, .returning = returning_columns };
    }

    fn conflictCaptures(self: *Parser, clause: *ast.Conflict) Error![]const *const ast.Scalar {
        const assignments = try self.alloc.dupe(ast.Assignment, clause.assignments);
        for (assignments) |*assignment| {
            const expression = assignment.expression orelse continue;
            if (!@import("subquery_lowering.zig").has(expression)) continue;
            // Direct scalar expressions are demand-masked by conflict owner
            // and DO UPDATE WHERE. Nested CASE/COALESCE branches need their
            // own row mask and remain unsupported until that vector operator
            // is bound. Never hoist either shape into the INSERT source.
            if (expression.* != .call or expression.call.subquery == null or !std.mem.eql(u8, expression.call.name, "$scalar") or expression.call.args.len != 0)
                return self.fail(error.UnsupportedSqlShape, "conflict assignment subquery requires post-owner masked Apply");
            assignment.deferred_scalar = true;
            clause.deferred_count += 1;
        }
        clause.assignments = assignments;
        return &.{};
    }

    fn conflict(self: *Parser) Error!?ast.Conflict {
        if (!self.keyword(.on)) return null;
        try self.expectKeyword(.conflict);
        var columns: std.ArrayList([]const u8) = .empty;
        var expressions: std.ArrayList(*const ast.Scalar) = .empty;
        const constraint_name = if (self.keyword(.on)) name: {
            try self.expectKeyword(.constraint);
            break :name try self.identifier();
        } else null;
        if (self.take(.lparen)) {
            if (constraint_name != null) return self.fail(error.InvalidSqlSyntax, "named conflict target cannot include inferred keys");
            while (true) {
                try self.node();
                const expression = try self.scalar(0, 0);
                if (expression.* == .column) try columns.append(self.alloc, expression.column) else try expressions.append(self.alloc, expression);
                if (!self.take(.comma)) break;
            }
            try self.expect(.rparen);
        }
        const arbiter_predicate = if (self.keyword(.where)) try self.scalar(0, 0) else null;
        if (constraint_name != null and arbiter_predicate != null) return self.fail(error.InvalidSqlSyntax, "named conflict target cannot include an inference predicate");
        if (arbiter_predicate != null and columns.items.len + expressions.items.len == 0) return self.fail(error.UnsupportedSqlShape, "partial conflict inference requires an explicit target");
        try self.expectKeyword(.do);
        if (self.keyword(.nothing)) return .{ .columns = try columns.toOwnedSlice(self.alloc), .constraint_name = constraint_name, .expressions = try expressions.toOwnedSlice(self.alloc), .arbiter_predicate = arbiter_predicate };
        if (constraint_name == null and columns.items.len + expressions.items.len == 0) return self.fail(error.UnsupportedSqlShape, "ON CONFLICT DO UPDATE requires an explicit conflict target");
        try self.expectKeyword(.update);
        try self.expectKeyword(.set);
        var assignments: std.ArrayList(ast.Assignment) = .empty;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        while (true) {
            const tuple = self.take(.lparen);
            try self.node();
            const first = try self.assignmentField();
            if ((try seen.getOrPut(self.alloc, first)).found_existing) return self.fail(error.DuplicateSqlAssignment, "duplicate conflict assignment");
            const targets: []const []const u8 = if (tuple) columns: {
                var names: std.ArrayList([]const u8) = .empty;
                try names.append(self.alloc, first);
                while (self.take(.comma)) {
                    try self.node();
                    const column = try self.assignmentField();
                    if ((try seen.getOrPut(self.alloc, column)).found_existing) return self.fail(error.DuplicateSqlAssignment, "duplicate conflict assignment");
                    try names.append(self.alloc, column);
                }
                try self.expect(.rparen);
                break :columns names.items;
            } else &.{first};
            try self.expect(.eq);
            try self.assignmentValues(targets, tuple, &assignments, true);
            if (!self.take(.comma)) break;
        }
        const filter = if (self.keyword(.where)) try self.scalar(0, 0) else null;
        if (filter) |expression| try self.checkScalarDepth(expression, 0);
        return .{ .columns = try columns.toOwnedSlice(self.alloc), .constraint_name = constraint_name, .expressions = try expressions.toOwnedSlice(self.alloc), .arbiter_predicate = arbiter_predicate, .assignments = try assignments.toOwnedSlice(self.alloc), .predicate = filter };
    }

    fn assignmentValues(self: *Parser, targets: []const []const u8, tuple: bool, assignments: *std.ArrayList(ast.Assignment), comptime literal_expression: bool) Error!void {
        if (tuple) {
            _ = self.keyword(.row);
            try self.expect(.lparen);
            if (self.pos < self.tokens.len and (self.tokens[self.pos].isKeyword(.select) or self.tokens[self.pos].isKeyword(.with))) {
                try self.rowSubqueryAssignments(targets, assignments);
                return;
            }
            for (targets, 0..) |column, i| {
                if (i != 0) try self.expect(.comma);
                if (self.keyword(.default)) {
                    try assignments.append(self.alloc, .{ .field = column, .use_default = true });
                    continue;
                }
                const expression = try self.scalar(0, 0);
                try self.checkScalarDepth(expression, 0);
                try assignments.append(self.alloc, if (!literal_expression and expression.* == .literal) .{ .field = column, .value = expression.literal } else .{ .field = column, .expression = expression });
            }
            if (!self.take(.rparen)) return self.fail(error.InvalidSqlSyntax, "row assignment value count does not match targets");
        } else {
            if (self.keyword(.default)) {
                try assignments.append(self.alloc, .{ .field = targets[0], .use_default = true });
            } else {
                const expression = try self.scalar(0, 0);
                try self.checkScalarDepth(expression, 0);
                try assignments.append(self.alloc, if (!literal_expression and expression.* == .literal) .{ .field = targets[0], .value = expression.literal } else .{ .field = targets[0], .expression = expression });
            }
        }
    }

    fn update(self: *Parser) Error!ast.Update {
        const table = try self.tableReferenceName();
        const alias = try self.sourceAlias();
        const target = try self.relationNode(.{ .table = .{ .name = table, .alias = alias, .mutation_target = true, .mutation_document = true, .mutation_presence = true } });
        var source = try self.relationTail(target);
        try self.expectKeyword(.set);
        var assignments = std.ArrayList(ast.Assignment).empty;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        while (true) {
            const tuple = self.take(.lparen);
            var targets: std.ArrayList([]const u8) = .empty;
            while (true) {
                try self.node();
                const column = try self.assignmentField();
                const entry = try seen.getOrPut(self.alloc, column);
                if (entry.found_existing) return self.fail(error.DuplicateSqlAssignment, "duplicate UPDATE assignment");
                try targets.append(self.alloc, column);
                if (!tuple or !self.take(.comma)) break;
            }
            if (tuple) try self.expect(.rparen);
            try self.expect(.eq);
            try self.assignmentValues(targets.items, tuple, &assignments, false);
            if (!self.take(.comma)) break;
        }
        if (self.keyword(.from)) {
            if (source != target) return self.fail(error.InvalidSqlSyntax, "use either joined UPDATE or UPDATE FROM");
            source = try self.relationNode(.{ .join = .{ .kind = .cross, .left = target, .right = try self.relation() } });
        }
        return .{ .table = table, .alias = alias, .source = if (alias != null or source != target) source else null, .assignments = try assignments.toOwnedSlice(self.alloc), .predicate = try self.where(), .returning = try self.returning() };
    }

    fn delete(self: *Parser) Error!ast.Delete {
        try self.expectKeyword(.from);
        const table = try self.tableReferenceName();
        const alias = try self.sourceAlias();
        const target = try self.relationNode(.{ .table = .{ .name = table, .alias = alias, .mutation_target = true, .mutation_presence = true } });
        var source = try self.relationTail(target);
        if (self.keyword(.using)) {
            if (source != target) return self.fail(error.InvalidSqlSyntax, "use either joined DELETE or DELETE USING");
            source = try self.relationNode(.{ .join = .{ .kind = .cross, .left = target, .right = try self.relation() } });
        }
        return .{ .table = table, .alias = alias, .source = if (alias != null or source != target) source else null, .predicate = try self.where(), .returning = try self.returning() };
    }

    fn merge(self: *Parser) Error!ast.Merge {
        try self.expectKeyword(.into);
        const table = try self.tableReferenceName();
        const alias = try self.sourceAlias();
        try self.expectKeyword(.using);
        const source = try self.relation();
        try self.expectKeyword(.on);
        const condition = try self.scalar(0, 0);
        try self.checkScalarDepth(condition, 0);
        var arms: std.ArrayList(ast.Merge.Arm) = .empty;
        while (self.keyword(.when)) {
            if (arms.items.len >= 128) return self.fail(error.SqlLimitExceeded, "MERGE arm limit exceeded");
            const matched = !self.keyword(.not);
            try self.expectKeyword(.matched);
            const arm_filter = if (self.keyword(.@"and")) try self.scalar(0, 0) else null;
            if (arm_filter) |filter_expr| try self.checkScalarDepth(filter_expr, 0);
            try self.expectKeyword(.then);
            var action: ast.Merge.Arm.Action = undefined;
            if (self.keyword(.update)) {
                if (!matched) return self.fail(error.InvalidSqlSyntax, "NOT MATCHED cannot UPDATE");
                try self.expectKeyword(.set);
                var assignments: std.ArrayList(ast.Assignment) = .empty;
                var seen: std.StringHashMapUnmanaged(void) = .empty;
                while (true) {
                    if (assignments.items.len >= 256) return self.fail(error.SqlLimitExceeded, "MERGE assignment limit exceeded");
                    try self.node();
                    const column = try self.assignmentField();
                    const entry = try seen.getOrPut(self.alloc, column);
                    if (entry.found_existing) return self.fail(error.DuplicateSqlAssignment, "duplicate MERGE assignment");
                    try self.expect(.eq);
                    if (self.keyword(.default)) {
                        try assignments.append(self.alloc, .{ .field = column, .use_default = true });
                        if (!self.take(.comma)) break;
                        continue;
                    }
                    const expression = try self.scalar(0, 0);
                    try self.checkScalarDepth(expression, 0);
                    try assignments.append(self.alloc, if (expression.* == .literal) .{ .field = column, .value = expression.literal } else .{ .field = column, .expression = expression });
                    if (!self.take(.comma)) break;
                }
                action = .{ .update = try assignments.toOwnedSlice(self.alloc) };
            } else if (self.keyword(.delete)) {
                if (!matched) return self.fail(error.InvalidSqlSyntax, "NOT MATCHED cannot DELETE");
                action = .delete;
            } else if (self.keyword(.insert)) {
                if (matched) return self.fail(error.InvalidSqlSyntax, "MATCHED cannot INSERT");
                try self.expect(.lparen);
                var columns: std.ArrayList([]const u8) = .empty;
                var seen: std.StringHashMapUnmanaged(void) = .empty;
                while (true) {
                    if (columns.items.len >= 256) return self.fail(error.SqlLimitExceeded, "MERGE insert column limit exceeded");
                    const column_name = try self.assignmentField();
                    const entry = try seen.getOrPut(self.alloc, column_name);
                    if (entry.found_existing) return self.fail(error.DuplicateSqlColumn, "duplicate MERGE insert column");
                    try columns.append(self.alloc, column_name);
                    if (!self.take(.comma)) break;
                }
                try self.expect(.rparen);
                try self.expectKeyword(.values);
                try self.expect(.lparen);
                var values: std.ArrayList(?*const ast.Scalar) = .empty;
                while (true) {
                    if (values.items.len >= columns.items.len) return self.fail(error.InvalidSqlSyntax, "MERGE insert value count exceeds columns");
                    const expression = if (self.keyword(.default)) null else try self.scalar(0, 0);
                    if (expression) |expr| try self.checkScalarDepth(expr, 0);
                    try values.append(self.alloc, expression);
                    if (!self.take(.comma)) break;
                }
                try self.expect(.rparen);
                if (values.items.len != columns.items.len) return self.fail(error.InvalidSqlSyntax, "MERGE insert value count differs from columns");
                action = .{ .insert = .{ .columns = try columns.toOwnedSlice(self.alloc), .values = try values.toOwnedSlice(self.alloc) } };
            } else if (self.keyword(.do)) {
                try self.expectKeyword(.nothing);
                action = .nothing;
            } else return self.fail(error.InvalidSqlSyntax, "expected MERGE action");
            try arms.append(self.alloc, .{ .matched = matched, .predicate = arm_filter, .action = action });
        }
        if (arms.items.len == 0) return self.fail(error.InvalidSqlSyntax, "MERGE requires a WHEN arm");
        return .{ .table = table, .alias = alias, .source = source, .condition = condition, .arms = try arms.toOwnedSlice(self.alloc), .returning = try self.returning() };
    }

    fn returning(self: *Parser) Error!?[]const ast.Projection {
        if (!self.keyword(.returning)) return null;
        var columns: std.ArrayList(ast.Projection) = .empty;
        while (true) {
            try columns.append(self.alloc, try self.parseProjection(false));
            if (!self.take(.comma)) break;
        }
        if (columns.items.len == 1 and columns.items[0].wildcard and columns.items[0].field.len == 0) columns.clearRetainingCapacity();
        return try columns.toOwnedSlice(self.alloc);
    }

    fn parseProjection(self: *Parser, implicit_alias: bool) Error!ast.Projection {
        try self.node();
        if (self.take(.star)) return .{ .wildcard = true };
        if (self.peek(.identifier)) {
            var end = self.pos;
            var parts: usize = 0;
            while (end + 2 < self.tokens.len and self.tokens[end].kind == .identifier and self.tokens[end + 1].kind == .dot) {
                parts += 1;
                if (self.tokens[end + 2].kind == .star) {
                    if (parts > 3) return self.fail(error.InvalidSqlSyntax, "wildcard qualification has at most three identifiers");
                    var qualifier = try self.identifier();
                    while (self.pos < end + 1) {
                        try self.expect(.dot);
                        qualifier = try std.fmt.allocPrint(self.alloc, "{s}\x00{s}", .{ qualifier, try self.identifier() });
                    }
                    try self.expect(.dot);
                    try self.expect(.star);
                    return .{ .field = qualifier, .wildcard = true };
                }
                end += 2;
            }
        }
        const expression = try self.scalar(0, 0);
        try self.checkScalarDepth(expression, 0);
        const alias = if (self.keyword(.as)) try self.identifier() else if (implicit_alias and self.peek(.identifier) and (self.tokens[self.pos].owned or self.tokens[self.pos].keyword == null or token.keywordClass(self.tokens[self.pos].keyword.?) == .unreserved)) try self.identifier() else null;
        // Keep the original PostgreSQL function/CASE label before lowering
        // replaces expressions with internal columns or aggregate slots.
        const label = alias orelse projectionLabel(expression);
        return if (expression.* == .column) .{ .field = expression.column, .alias = alias } else .{ .expression = expression, .alias = label };
    }

    // Type casts preserve a named operand's PostgreSQL output label. Capture
    // it before relational lowering substitutes internal expression columns.
    fn projectionLabel(expression: *const ast.Scalar) ?[]const u8 {
        return switch (expression.*) {
            .call => |call| if (std.mem.eql(u8, call.name, "$current_timestamp")) "current_timestamp" else if (std.mem.eql(u8, call.name, "$current_date")) "current_date" else if (std.mem.eql(u8, call.name, "$current_user")) "current_user" else if (std.mem.eql(u8, call.name, "$regex_operator")) "?column?" else if (std.mem.startsWith(u8, call.name, "$")) null else if (std.mem.lastIndexOfScalar(u8, call.name, 0)) |at| call.name[at + 1 ..] else call.name,
            .case_when => "case",
            .column => |column| if (std.mem.lastIndexOfScalar(u8, column, 0)) |at| column[at + 1 ..] else column,
            .cast => |cast| switch (cast.operand.*) {
                .column, .call, .cast => projectionLabel(cast.operand),
                else => null,
            },
            else => null,
        };
    }

    fn patternArray(self: *Parser, operand: *const ast.Scalar, input: *const ast.Scalar, every: bool, insensitive: bool, negated: bool) Error!*const ast.Scalar {
        const all_node = try self.scalarNode(.{ .literal = .{ .boolean = every } });
        const insensitive_node = try self.scalarNode(.{ .literal = .{ .boolean = insensitive } });
        const negated_node = try self.scalarNode(.{ .literal = .{ .boolean = negated } });
        return self.scalarNode(.{ .call = .{ .name = "$array_pattern_quantified", .args = try self.alloc.dupe(*const ast.Scalar, &.{ operand, input, all_node, insensitive_node, negated_node }) } });
    }

    const CastType = struct { type: ast.ColumnType, temporal_kind: ?@import("temporal_value.zig").Kind = null, element_type: ?@import("array_value.zig").ElementType = null, numeric_modifier: ?@import("../common/sql_builtin_type.zig").NumericModifier = null };

    fn numericModifier(self: *Parser) Error!?@import("../common/sql_builtin_type.zig").NumericModifier {
        if (!self.take(.lparen)) return null;
        var arguments: [2][]const u8 = undefined;
        var count: usize = 0;
        while (true) {
            const negative = self.take(.minus);
            if (negative and !self.peek(.number)) return self.fail(error.InvalidSqlSyntax, "expected numeric type modifier");
            const text = if (self.peek(.number) or self.peek(.string)) text: {
                const current = self.tokens[self.pos];
                self.pos += 1;
                break :text current.text;
            } else if (self.peek(.identifier)) try self.identifier() else return self.fail(error.InvalidSqlSyntax, "expected numeric type modifier");
            if (count < arguments.len) arguments[count] = if (negative) try std.fmt.allocPrint(self.alloc, "-{s}", .{text}) else text;
            count += 1;
            if (!self.take(.comma)) break;
        }
        try self.expect(.rparen);
        if (count == 0 or count > 2) return self.fail(error.SqlInvalidParameterValue, "NUMERIC requires precision and optional scale");
        const casts = @import("builtin_cast.zig");
        const precision = casts.integerText(arguments[0], .int32) catch |err| return self.fail(if (err == error.SqlNumericOutOfRange) error.SqlNumericOutOfRange else error.SqlInvalidTextRepresentation, "invalid NUMERIC precision");
        const scale = if (count == 2) casts.integerText(arguments[1], .int32) catch |err| return self.fail(if (err == error.SqlNumericOutOfRange) error.SqlNumericOutOfRange else error.SqlInvalidTextRepresentation, "invalid NUMERIC scale") else 0;
        if (precision < 1 or precision > 1000 or scale < -1000 or scale > 1000) return self.fail(error.SqlInvalidParameterValue, "NUMERIC precision must be 1..1000 and scale -1000..1000");
        return .{ .precision = @intCast(precision), .scale = @intCast(scale) };
    }

    fn castType(self: *Parser) Error!CastType {
        const start = self.pos;
        if (!self.peek(.identifier) or self.tokens[self.pos].owned) return .{ .type = try self.columnType() };
        const name_value = self.tokens[self.pos].text;
        self.pos += 1;
        if (std.ascii.eqlIgnoreCase(name_value, "time")) return .{ .type = .datetime, .temporal_kind = .time };
        if (std.ascii.eqlIgnoreCase(name_value, "timetz")) return .{ .type = .datetime, .temporal_kind = .time_tz };
        if (std.ascii.eqlIgnoreCase(name_value, "interval")) return .{ .type = .datetime, .temporal_kind = .interval };
        if (std.ascii.eqlIgnoreCase(name_value, "date")) return .{ .type = .datetime, .temporal_kind = .date };
        if (std.ascii.eqlIgnoreCase(name_value, "timestamp")) return .{ .type = .datetime, .temporal_kind = .timestamp };
        const pairs = .{
            .{ "text", @import("array_value.zig").ElementType.text },
            .{ "uuid", @import("array_value.zig").ElementType.uuid },
            .{ "smallint", @import("array_value.zig").ElementType.int16 },
            .{ "int2", @import("array_value.zig").ElementType.int16 },
            .{ "int", @import("array_value.zig").ElementType.int32 },
            .{ "integer", @import("array_value.zig").ElementType.int32 },
            .{ "int4", @import("array_value.zig").ElementType.int32 },
            .{ "bigint", @import("array_value.zig").ElementType.int64 },
            .{ "int8", @import("array_value.zig").ElementType.int64 },
            .{ "real", @import("array_value.zig").ElementType.float32 },
            .{ "float4", @import("array_value.zig").ElementType.float32 },
            .{ "float8", @import("array_value.zig").ElementType.float64 },
            .{ "double", @import("array_value.zig").ElementType.float64 },
            .{ "bool", @import("array_value.zig").ElementType.boolean },
            .{ "boolean", @import("array_value.zig").ElementType.boolean },
            .{ "jsonb", @import("array_value.zig").ElementType.jsonb },
            .{ "numeric", @import("array_value.zig").ElementType.numeric },
            .{ "decimal", @import("array_value.zig").ElementType.numeric },
        };
        var element: ?@import("array_value.zig").ElementType = null;
        inline for (pairs) |pair| if (std.ascii.eqlIgnoreCase(name_value, pair[0])) {
            element = pair[1];
        };
        if (std.ascii.eqlIgnoreCase(name_value, "double")) {
            if (!self.ddlWord("precision")) return self.fail(error.InvalidSqlSyntax, "expected DOUBLE PRECISION");
        }
        const numeric_modifier = if (element == .numeric) try self.numericModifier() else null;
        if (!self.take(.lbracket)) {
            if (element) |resolved| return .{ .type = switch (resolved) {
                .text => .string,
                .uuid => .uuid,
                .int16, .int32, .int64 => .integer,
                .float32, .float64, .numeric => .number,
                .boolean => .boolean,
                .jsonb => .json,
            }, .element_type = resolved, .numeric_modifier = numeric_modifier };
            self.pos = start;
            return .{ .type = try self.columnType() };
        }
        const resolved = element orelse return self.fail(error.UnsupportedSqlShape, "unsupported SQL array element type");
        // Declared dimensions do not participate in PostgreSQL type identity
        // and do not constrain a value's actual rank or bounds.
        while (true) {
            if (self.peek(.number)) {
                const size = try self.value();
                if (size != .integer or size.integer < 0 or size.integer > std.math.maxInt(i32)) return self.fail(error.InvalidSqlSyntax, "invalid declared array dimension");
            }
            try self.expect(.rbracket);
            if (!self.take(.lbracket)) break;
        }
        return .{ .type = .array, .element_type = resolved, .numeric_modifier = numeric_modifier };
    }

    fn columnType(self: *Parser) Error!ast.ColumnType {
        if (!self.peek(.identifier)) return self.fail(error.InvalidSqlSyntax, "expected SQL column type");
        const t = self.tokens[self.pos];
        if (t.keyword) |word| if (token.keywordClass(word) == .reserved)
            return self.fail(error.InvalidSqlSyntax, "reserved keyword is not a column type");
        self.pos += 1;
        // Quoted type names denote user-defined types, which require catalog
        // lookup; never silently reinterpret them as builtins.
        if (t.owned) return self.fail(error.UnsupportedSqlShape, "user-defined column types are not supported");
        const pairs = .{
            .{ "text", ast.ColumnType.string },          .{ "string", ast.ColumnType.string },
            .{ "uuid", ast.ColumnType.uuid },            .{ "bigint", ast.ColumnType.integer },
            .{ "int8", ast.ColumnType.integer },         .{ "integer", ast.ColumnType.integer },
            .{ "int", ast.ColumnType.integer },          .{ "float8", ast.ColumnType.number },
            .{ "number", ast.ColumnType.number },        .{ "boolean", ast.ColumnType.boolean },
            .{ "bool", ast.ColumnType.boolean },         .{ "datetime", ast.ColumnType.datetime },
            .{ "numrange", ast.ColumnType.numrange },    .{ "daterange", ast.ColumnType.daterange },
            .{ "tsrange", ast.ColumnType.tsrange },      .{ "tstzrange", ast.ColumnType.tstzrange },
            .{ "timestamptz", ast.ColumnType.datetime }, .{ "json", ast.ColumnType.json },
            .{ "jsonb", ast.ColumnType.json },
        };
        inline for (pairs) |pair| if (std.ascii.eqlIgnoreCase(t.text, pair[0])) return pair[1];
        if (std.ascii.eqlIgnoreCase(t.text, "double")) {
            if (!self.peek(.identifier) or self.tokens[self.pos].owned or !std.ascii.eqlIgnoreCase(self.tokens[self.pos].text, "precision")) return self.fail(error.InvalidSqlSyntax, "expected DOUBLE PRECISION");
            self.pos += 1;
            return .number;
        }
        return self.fail(error.UnsupportedSqlShape, "unsupported SQL column type");
    }

    fn columnCollation(self: *Parser, column: ast.Column) Error!void {
        const collation = try self.name();
        // Native text comparisons, key tuples and array elements have a fixed
        // binary domain. These two PostgreSQL names normalize to that same
        // domain; never accept a locale or case-folding name and discard it.
        if (collation.database != null or (collation.namespace != null and
            !std.mem.eql(u8, collation.namespace.?, "pg_catalog")) or
            (!std.mem.eql(u8, collation.table, "C") and !std.mem.eql(u8, collation.table, "POSIX")))
            return self.fail(error.UnsupportedSqlShape, "column collation requires the native binary C or POSIX domain");
        if (column.type != .string and !(column.type == .array and column.element_type == .text))
            return self.fail(error.UnsupportedSqlShape, "column type does not support a text collation");
    }

    fn columnDefinition(self: *Parser, name_value: []const u8) Error!ast.Column {
        if (self.pos < self.tokens.len and self.tokens[self.pos].kind == .identifier and !self.tokens[self.pos].owned) {
            const serial_word = self.tokens[self.pos].text;
            const integer: ?@import("../system_catalog/sequences.zig").Integer =
                if (std.ascii.eqlIgnoreCase(serial_word, "smallserial") or std.ascii.eqlIgnoreCase(serial_word, "serial2")) .int16 else if (std.ascii.eqlIgnoreCase(serial_word, "serial") or std.ascii.eqlIgnoreCase(serial_word, "serial4")) .int32 else if (std.ascii.eqlIgnoreCase(serial_word, "bigserial") or std.ascii.eqlIgnoreCase(serial_word, "serial8")) .int64 else null;
            if (integer) |width| {
                self.pos += 1;
                return .{ .name = name_value, .type = .integer, .element_type = switch (width) {
                    .int16 => .int16,
                    .int32 => .int32,
                    .int64 => .int64,
                }, .nullable = false, .sequence = .{ .column = name_value, .generation = .serial, .options = .{ .integer = width } } };
            }
        }
        const declared = try self.castType();
        if (declared.temporal_kind != null) return self.fail(error.UnsupportedSqlShape, "persistent temporal subtype requires native storage support");
        return .{ .name = name_value, .type = declared.type, .element_type = declared.element_type, .numeric_modifier = declared.numeric_modifier };
    }

    pub fn createTable(self: *Parser) Error!ast.CreateTable {
        const if_not_exists = self.keyword(.@"if");
        if (if_not_exists) {
            try self.expectKeyword(.not);
            try self.expectKeyword(.exists);
        }
        const table = try self.name();
        try self.expect(.lparen);
        var columns = std.ArrayList(ast.Column).empty;
        var constraints = std.ArrayList(ast.SchemaChange).empty;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        if (!self.peek(.rparen)) {
            while (true) {
                try self.node();
                if (self.keyword(.constraint)) {
                    try constraints.append(self.alloc, try self.tableConstraint());
                    if (!self.take(.comma)) break;
                    continue;
                }
                if (self.peek(.identifier) and (self.tokens[self.pos].isKeyword(.primary) or self.tokens[self.pos].isKeyword(.unique) or self.tokens[self.pos].isKeyword(.check) or self.tokens[self.pos].isKeyword(.foreign))) {
                    const constraint_name = try std.fmt.allocPrint(self.alloc, "sql_constraint_{d}", .{constraints.items.len});
                    try constraints.append(self.alloc, generatedConstraint(try self.constraintDefinition(constraint_name)));
                    if (!self.take(.comma)) break;
                    continue;
                }
                const column = try self.identifier();
                if (std.mem.eql(u8, column, "_id")) return self.fail(error.UnsupportedSqlShape, "_id is reserved for row identity");
                const entry = try seen.getOrPut(self.alloc, column);
                if (entry.found_existing) return self.fail(error.DuplicateSqlColumn, "duplicate CREATE TABLE column");
                var definition = try self.columnDefinition(column);
                var collation_seen = false;
                var null_seen = false;
                var default_seen = false;
                while (true) {
                    if (self.keyword(.collate)) {
                        if (collation_seen) return self.fail(error.InvalidSqlSyntax, "duplicate column collation");
                        try self.columnCollation(definition);
                        collation_seen = true;
                    } else if (self.keyword(.not)) {
                        if (null_seen) return self.fail(error.InvalidSqlSyntax, "duplicate column nullability");
                        try self.expectKeyword(.null);
                        definition.nullable = false;
                        null_seen = true;
                    } else if (self.keyword(.null)) {
                        if (null_seen) return self.fail(error.InvalidSqlSyntax, "duplicate column nullability");
                        if (definition.sequence != null) return self.fail(error.InvalidSqlSyntax, "sequence-generated columns cannot declare NULL");
                        null_seen = true;
                    } else if (self.keyword(.default)) {
                        if (default_seen or definition.generated_expression != null or definition.sequence != null) return self.fail(error.InvalidSqlSyntax, "duplicate or conflicting column default");
                        definition.default_expression = try self.schemaDefault();
                        default_seen = true;
                    } else if (self.ddlWord("generated")) {
                        if (default_seen or definition.generated_expression != null or definition.sequence != null) return self.fail(error.InvalidSqlSyntax, "duplicate or conflicting generated column");
                        try self.generatedColumn(&definition, null_seen and definition.nullable);
                    } else if (self.keyword(.primary)) {
                        try self.expectKeyword(.key);
                        definition.nullable = false;
                        try constraints.append(self.alloc, try self.uniqueTiming(.{ .add_unique = .{ .name = try std.fmt.allocPrint(self.alloc, "sql_primary_{d}", .{constraints.items.len}), .columns = try self.alloc.dupe([]const u8, &.{column}), .name_generated = true, .primary = true } }));
                    } else if (self.keyword(.unique)) {
                        const nulls_not_distinct = try self.uniqueNulls();
                        try constraints.append(self.alloc, try self.uniqueTiming(.{ .add_unique = .{ .name = try std.fmt.allocPrint(self.alloc, "sql_unique_{d}", .{constraints.items.len}), .columns = try self.alloc.dupe([]const u8, &.{column}), .name_generated = true, .nulls_not_distinct = nulls_not_distinct } }));
                    } else if (self.keyword(.constraint)) {
                        const constraint_name = try self.identifier();
                        try constraints.append(self.alloc, try self.inlineConstraint(constraint_name, column));
                    } else if (self.peek(.identifier) and !self.tokens[self.pos].owned and (self.tokens[self.pos].isKeyword(.check) or std.ascii.eqlIgnoreCase(self.tokens[self.pos].text, "REFERENCES"))) {
                        const constraint_name = try std.fmt.allocPrint(self.alloc, "sql_inline_{d}", .{constraints.items.len});
                        try constraints.append(self.alloc, generatedConstraint(try self.inlineConstraint(constraint_name, column)));
                    } else break;
                }
                try columns.append(self.alloc, definition);
                if (!self.take(.comma)) break;
            }
        }
        try self.expect(.rparen);
        const document = try self.documentTableOptions(columns.items.len, constraints.items.len);
        const tablespace = if (self.keyword(.tablespace)) try self.identifier() else null;
        const document_definition = try self.documentTableSchemas(document);
        var has_primary = false;
        for (constraints.items) |constraint| if (constraint == .add_unique and constraint.add_unique.primary) {
            if (has_primary) return self.fail(error.InvalidSqlSyntax, "a table can have only one primary key");
            has_primary = true;
            for (constraint.add_unique.columns) |key| {
                for (columns.items) |*definition| {
                    if (std.mem.eql(u8, key, definition.name)) {
                        definition.nullable = false;
                        break;
                    }
                } else return self.fail(error.InvalidSqlSyntax, "primary key references an unknown column");
            }
        };
        try self.normalizeUniqueConstraints(&constraints);
        try self.disambiguateGeneratedConstraintLabels(constraints.items);
        return .{ .table = table, .columns = try columns.toOwnedSlice(self.alloc), .constraints = try constraints.toOwnedSlice(self.alloc), .if_not_exists = if_not_exists, .tablespace = tablespace, .document = document_definition };
    }

    const DocumentOptions = struct { enabled: bool = false, default_type: ?[]const u8 = null, enforce_types: bool = false };

    fn documentTableOptions(self: *Parser, columns: usize, constraints: usize) Error!DocumentOptions {
        var result: DocumentOptions = .{};
        if (!self.keyword(.with)) return result;
        try self.expect(.lparen);
        var seen: u3 = 0;
        while (true) {
            try self.node();
            if (!self.ddlWord("antfly")) return self.fail(error.UnsupportedSqlShape, "unknown CREATE TABLE storage option namespace");
            try self.expect(.dot);
            const option = try self.identifier();
            const bit: u3 = if (std.mem.eql(u8, option, "storage_mode")) 1 else if (std.mem.eql(u8, option, "default_type")) 2 else if (std.mem.eql(u8, option, "enforce_types")) 4 else return self.fail(error.UnsupportedSqlShape, "unknown Antfly table option");
            if (seen & bit != 0) return self.fail(error.SqlInvalidParameterValue, "duplicate Antfly table option");
            seen |= bit;
            try self.expect(.eq);
            if (bit == 4) {
                result.enforce_types = if (self.keyword(.true)) true else if (self.keyword(.false)) false else return self.fail(error.InvalidSqlSyntax, "enforce_types requires TRUE or FALSE");
            } else {
                if (!self.peek(.string)) return self.fail(error.InvalidSqlSyntax, "Antfly table option requires a string");
                const value_ = self.tokens[self.pos].text;
                self.pos += 1;
                if (bit == 1) {
                    if (!std.mem.eql(u8, value_, "document")) return self.fail(error.UnsupportedSqlShape, "explicit Antfly storage options require document mode");
                    result.enabled = true;
                } else result.default_type = try self.alloc.dupe(u8, value_);
            }
            if (!self.take(.comma)) break;
        }
        try self.expect(.rparen);
        if (!result.enabled) return self.fail(error.InvalidSqlSyntax, "document options require antfly.storage_mode = 'document'");
        if (columns != 0 or constraints != 0) return self.fail(error.UnsupportedSqlShape, "document tables declare DOCUMENT SCHEMA rather than SQL row columns or constraints");
        return result;
    }

    fn documentTableSchemas(self: *Parser, options: DocumentOptions) Error!?*const ast.DocumentTable {
        if (!options.enabled) {
            if (self.ddlWord("DOCUMENT")) return self.fail(error.InvalidSqlSyntax, "DOCUMENT SCHEMA requires explicit document storage mode");
            return null;
        }
        var schemas = std.ArrayList(@typeInfo(@FieldType(ast.DocumentTable, "schemas")).pointer.child).empty;
        var names: std.StringHashMapUnmanaged(void) = .empty;
        while (self.ddlWord("DOCUMENT")) {
            try self.node();
            try self.expectKeyword(.schema);
            const name_ = try self.identifier();
            if (name_.len == 0 or names.contains(name_)) return self.fail(error.SqlInvalidParameterValue, "empty or duplicate document schema name");
            if (schemas.items.len == 256) return self.fail(error.SqlLimitExceeded, "too many document schemas");
            try names.put(self.alloc, name_, {});
            try self.expectKeyword(.as);
            if (!self.ddlWord("JSON") or !self.peek(.string)) return self.fail(error.InvalidSqlSyntax, "DOCUMENT SCHEMA requires AS JSON followed by a string");
            const schema_json = try self.alloc.dupe(u8, self.tokens[self.pos].text);
            self.pos += 1;
            try schemas.append(self.alloc, .{ .name = name_, .schema_json = schema_json });
        }
        // SQL declarations bind one typed document layout. Schemaless tables
        // remain available without declarations; multi-type native API tables
        // are not silently flattened into an arbitrary SQL column contract.
        if (schemas.items.len > 1) return self.fail(error.UnsupportedSqlShape, "SQL document declarations require one default document schema");
        if (schemas.items.len != 0 and (options.default_type == null or options.default_type.?.len == 0))
            return self.fail(error.SqlInvalidParameterValue, "DOCUMENT SCHEMA requires an explicit default_type");
        if (options.default_type) |name_| {
            if (name_.len != 0 and !names.contains(name_)) return self.fail(error.SqlInvalidParameterValue, "default document type has no schema declaration");
        }
        if (self.keyword(.with)) return self.fail(error.InvalidSqlSyntax, "table options must precede DOCUMENT SCHEMA; a trailing WITH clause is not valid");
        const result = try self.alloc.create(ast.DocumentTable);
        result.* = .{ .default_type = options.default_type, .enforce_types = options.enforce_types, .schemas = try schemas.toOwnedSlice(self.alloc) };
        return result;
    }

    /// Match PostgreSQL's CREATE index-constraint normalization before name
    /// selection: PRIMARY wins equivalent UNIQUE declarations; otherwise the
    /// first equivalent index wins. A named duplicate can supply an anonymous
    /// survivor's name. Policy differences and ordered keys remain distinct.
    fn normalizeUniqueConstraints(self: *Parser, constraints: *std.ArrayList(ast.SchemaChange)) Error!void {
        const Rule = @FieldType(ast.SchemaChange, "add_unique");
        const Context = struct {
            pub fn hash(_: @This(), rule: Rule) u64 {
                var digest = std.hash.Wyhash.init(0);
                digest.update(&.{ @intFromBool(rule.nulls_not_distinct), @intFromBool(rule.deferrable) });
                digest.update(rule.timing);
                digest.update(&.{@intFromBool(rule.without_overlaps_column != null)});
                if (rule.without_overlaps_column) |column| digest.update(column);
                var length: [8]u8 = undefined;
                for (rule.columns) |column| {
                    std.mem.writeInt(u64, &length, column.len, .little);
                    digest.update(&length);
                    digest.update(column);
                }
                digest.update("include");
                for (rule.include_columns) |column| {
                    std.mem.writeInt(u64, &length, column.len, .little);
                    digest.update(&length);
                    digest.update(column);
                }
                return digest.final();
            }
            pub fn eql(_: @This(), left: Rule, right: Rule) bool {
                if (left.nulls_not_distinct != right.nulls_not_distinct or left.deferrable != right.deferrable or
                    !std.mem.eql(u8, left.timing, right.timing) or left.columns.len != right.columns.len or
                    left.include_columns.len != right.include_columns.len) return false;
                if ((left.without_overlaps_column == null) != (right.without_overlaps_column == null)) return false;
                if (left.without_overlaps_column) |period| if (!std.mem.eql(u8, period, right.without_overlaps_column.?)) return false;
                for (left.columns, right.columns) |l, r| if (!std.mem.eql(u8, l, r)) return false;
                for (left.include_columns, right.include_columns) |l, r| if (!std.mem.eql(u8, l, r)) return false;
                return true;
            }
        };
        var count: usize = 0;
        for (constraints.items) |constraint| if (constraint == .add_unique) {
            count += 1;
        };
        if (count < 2) return;
        var indexes: std.HashMapUnmanaged(Rule, usize, Context, 80) = .empty;
        defer indexes.deinit(self.alloc);
        // Grow with distinct indexes, not redundant declarations. Thousands
        // of equivalent clauses should not preallocate thousands of entries.
        try indexes.ensureTotalCapacity(self.alloc, @intCast(@min(count, 16)));
        const keep = try self.alloc.alloc(bool, constraints.items.len);
        defer self.alloc.free(keep);
        @memset(keep, true);
        // Seed PRIMARY first even if it was declared after a UNIQUE.
        for (constraints.items, 0..) |constraint, index| if (constraint == .add_unique and constraint.add_unique.primary) {
            indexes.putAssumeCapacity(constraint.add_unique, index);
        };
        for (constraints.items, 0..) |constraint, index| {
            if (constraint != .add_unique or constraint.add_unique.primary) continue;
            const slot = try indexes.getOrPut(self.alloc, constraint.add_unique);
            if (!slot.found_existing) {
                slot.value_ptr.* = index;
                continue;
            }
            const survivor = &constraints.items[slot.value_ptr.*].add_unique;
            if (survivor.name_generated and !constraint.add_unique.name_generated) {
                survivor.name = constraint.add_unique.name;
                survivor.name_generated = false;
            }
            keep[index] = false;
        }
        var length: usize = 0;
        for (constraints.items, keep) |constraint, retained| if (retained) {
            constraints.items[length] = constraint;
            length += 1;
        };
        constraints.items.len = length;
    }

    /// These are compiler identities, not final catalog names. Reserve every
    /// explicit spelling before assigning generated identities, including
    /// declarations later in the statement. Never rename an explicit name or
    /// infer provenance from a prefix. A single increasing fallback counter
    /// bounds aggregate collision probes by the number of reserved labels.
    fn disambiguateGeneratedConstraintLabels(self: *Parser, constraints: []ast.SchemaChange) Error!void {
        var has_generated = false;
        for (constraints) |constraint| if (constraint.generatedConstraintName() != null) {
            has_generated = true;
            break;
        };
        if (!has_generated) return;
        var occupied: std.StringHashMapUnmanaged(bool) = .empty;
        defer occupied.deinit(self.alloc);
        // False reserves an original generated spelling; true means explicit
        // or already assigned. At most one fallback is added per constraint.
        try occupied.ensureTotalCapacity(self.alloc, @intCast(constraints.len * 2));
        for (constraints) |constraint| {
            if (constraint.generatedConstraintName() != null) continue;
            const label = switch (constraint) {
                .add_unique => |rule| rule.name,
                .add_check => |rule| rule.name,
                .add_foreign_key => |rule| rule.name,
                else => continue,
            };
            // Duplicate explicit names remain an admission error; this pass
            // must not hide them by changing the user's declaration.
            occupied.putAssumeCapacity(label, true);
        }
        // Reserve existing generated spellings too, so a fallback cannot
        // steal a later generated identity and cause unnecessary renaming.
        for (constraints) |constraint| if (constraint.generatedConstraintName()) |label| {
            if (!occupied.contains(label)) occupied.putAssumeCapacity(label, false);
        };
        var suffix: usize = 0;
        for (constraints) |*constraint| {
            const original = constraint.generatedConstraintName() orelse continue;
            const assigned = occupied.getPtr(original).?;
            if (!assigned.*) {
                assigned.* = true;
                continue;
            }
            var buffer: [48]u8 = undefined;
            const replacement = while (true) {
                const label = std.fmt.bufPrint(&buffer, "sql_generated_{d}", .{suffix}) catch unreachable;
                suffix += 1;
                if (!occupied.contains(label)) break try self.alloc.dupe(u8, label);
            };
            occupied.putAssumeCapacity(replacement, true);
            switch (constraint.*) {
                .add_unique => |*rule| rule.name = replacement,
                .add_check => |*rule| rule.name = replacement,
                .add_foreign_key => |*rule| rule.name = replacement,
                else => unreachable,
            }
        }
    }

    fn viewDdl(self: *Parser, action: @FieldType(ast.CatalogDdl, "action"), replace: bool) Error!ast.CatalogDdl {
        try self.node();
        var conditional = false;
        if (action == .drop or action == .rename) {
            conditional = self.keyword(.@"if");
            if (conditional) try self.expectKeyword(.exists);
        } else if (self.keyword(.@"if")) {
            return self.fail(error.InvalidSqlSyntax, "CREATE VIEW does not support IF NOT EXISTS");
        }
        var ddl: ast.CatalogDdl = .{ .kind = .view, .action = action, .name = try self.name(), .conditional = conditional };
        if (action == .drop) {
            ddl.cascade = self.ddlWord("CASCADE");
            if (!ddl.cascade) _ = self.ddlWord("RESTRICT");
            return ddl;
        }
        if (action == .rename) {
            if (!self.keyword(.rename)) return self.fail(error.UnsupportedSqlShape, "ALTER VIEW requires RENAME TO");
            try self.expectKeyword(.to);
            ddl.new_name = try self.identifier();
            return ddl;
        }
        const columns = if (self.peek(.lparen)) try self.ddlColumnList() else &.{};
        if (columns.len > 1024) return self.fail(error.SqlLimitExceeded, "view output column limit exceeded");
        var invoker = false;
        var barrier = false;
        var seen: u2 = 0;
        if (self.keyword(.with)) {
            try self.expect(.lparen);
            while (true) {
                const option_name = try self.identifier();
                const bit: u2 = if (std.mem.eql(u8, option_name, "security_invoker")) 1 else if (std.mem.eql(u8, option_name, "security_barrier")) 2 else return self.fail(error.UnsupportedSqlShape, "unsupported view option");
                if (seen & bit != 0) return self.fail(error.SqlInvalidParameterValue, "duplicate view option");
                seen |= bit;
                try self.expect(.eq);
                const enabled = if (self.keyword(.true)) true else if (self.keyword(.false)) false else return self.fail(error.InvalidSqlSyntax, "view security option requires TRUE or FALSE");
                if (bit == 1) invoker = enabled else barrier = enabled;
                if (!self.take(.comma)) break;
            }
            try self.expect(.rparen);
        }
        try self.expectKeyword(.as);
        if (self.pos == self.tokens.len) return self.fail(error.InvalidSqlSyntax, "view requires a SELECT query");
        const start = self.tokens[self.pos].source_start;
        const parameters = self.parameter_count;
        self.relation_depth += 1;
        defer self.relation_depth -= 1;
        if (self.relation_depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL query nesting limit exceeded");
        const statement_ = try self.statement();
        if (statement_ != .select) return self.fail(error.InvalidSqlSyntax, "view requires a SELECT query");
        if (self.parameter_count != parameters) return self.fail(error.InvalidSqlParameter, "view query cannot contain parameters");
        const end = self.tokens[self.pos - 1].source_end;
        const query = try self.alloc.create(ast.Select);
        query.* = statement_.select;
        ddl.view_definition = .{ .query = query, .query_sql = try self.alloc.dupe(u8, self.source[start..end]), .columns = columns, .replace = replace, .security_invoker = invoker, .security_barrier = barrier };
        return ddl;
    }

    fn sequenceInteger(self: *Parser) Error!i64 {
        const negative = self.take(.minus);
        if (!negative) _ = self.take(.plus);
        if (!self.peek(.number)) return self.fail(error.InvalidSqlSyntax, "sequence options require integer constants");
        const text = self.tokens[self.pos].text;
        self.pos += 1;
        const magnitude = parseIntegerToken(u64, text) catch |err| return self.fail(if (err == error.Overflow) error.SqlNumericOutOfRange else error.SqlInvalidTextRepresentation, "sequence option is not a signed 64-bit integer");
        if (negative) {
            if (magnitude == @as(u64, std.math.maxInt(i64)) + 1) return std.math.minInt(i64);
            if (magnitude > std.math.maxInt(i64)) return self.fail(error.SqlNumericOutOfRange, "sequence integer is out of range");
            return -@as(i64, @intCast(magnitude));
        }
        return std.math.cast(i64, magnitude) orelse self.fail(error.SqlNumericOutOfRange, "sequence integer is out of range");
    }

    fn sequenceOptions(self: *Parser, mode: enum { create, alter, identity }) Error!ast.SequenceOptions {
        var options: ast.SequenceOptions = .{};
        var seen: u16 = 0;
        while (self.pos < self.tokens.len and !self.peek(.semicolon) and (mode != .identity or !self.peek(.rparen))) {
            const bit: u16 = if (self.keyword(.as)) 1 else if (self.ddlWord("INCREMENT")) 2 else if (self.ddlWord("MINVALUE")) 4 else if (self.ddlWord("MAXVALUE")) 8 else if (self.keyword(.start)) 16 else if (self.ddlWord("CACHE")) 32 else if (self.ddlWord("CYCLE")) 64 else if (self.ddlWord("OWNED")) 128 else if (self.keyword(.restart)) 256 else if (self.ddlWord("NO")) {
                const which: u16 = if (self.ddlWord("MINVALUE")) 4 else if (self.ddlWord("MAXVALUE")) 8 else if (self.ddlWord("CYCLE")) 64 else return self.fail(error.InvalidSqlSyntax, "NO requires MINVALUE, MAXVALUE or CYCLE");
                if (seen & which != 0) return self.fail(error.InvalidSqlSyntax, "conflicting or redundant sequence option");
                seen |= which;
                if (which == 4) options.values.minimum = .default else if (which == 8) options.values.maximum = .default else options.values.cycle = false;
                continue;
            } else return self.fail(error.UnsupportedSqlShape, "unsupported sequence alteration or option");
            if (mode == .identity and (bit == 1 or bit == 128 or bit == 256)) return self.fail(error.InvalidSqlSyntax, "invalid identity sequence option");
            if (seen & bit != 0) return self.fail(error.InvalidSqlSyntax, "conflicting or redundant sequence option");
            seen |= bit;
            switch (bit) {
                1 => {
                    options.data_type_quoted = self.pos < self.tokens.len and self.tokens[self.pos].owned;
                    options.data_type = try self.name();
                },
                2 => {
                    _ = self.keyword(.by);
                    options.values.increment = try self.sequenceInteger();
                },
                4 => options.values.minimum = .{ .value = try self.sequenceInteger() },
                8 => options.values.maximum = .{ .value = try self.sequenceInteger() },
                16 => {
                    _ = self.keyword(.with);
                    options.values.start = try self.sequenceInteger();
                },
                32 => options.cache = try self.sequenceInteger(),
                64 => options.values.cycle = true,
                128 => {
                    try self.expectKeyword(.by);
                    if (self.ddlWord("NONE")) {
                        options.owned_by = .none;
                    } else {
                        var parts: [4][]const u8 = undefined;
                        var count: usize = 1;
                        parts[0] = try self.identifier();
                        while (self.take(.dot)) {
                            if (count == parts.len) return self.fail(error.InvalidSqlSyntax, "OWNED BY requires table.column");
                            parts[count] = try self.identifier();
                            count += 1;
                        }
                        if (count < 2) return self.fail(error.InvalidSqlSyntax, "OWNED BY requires table.column");
                        options.owned_by = .{ .column = .{ .column = parts[count - 1], .table = .{
                            .table = parts[count - 2],
                            .namespace = if (count >= 3) parts[count - 3] else null,
                            .database = if (count == 4) parts[0] else null,
                        } } };
                    }
                },
                256 => {
                    const with = self.keyword(.with);
                    const has_value = self.peek(.number) or self.peek(.plus) or self.peek(.minus);
                    if (with and !has_value) return self.fail(error.InvalidSqlSyntax, "RESTART WITH requires an integer");
                    options.values.restart = .{ .value = if (has_value) try self.sequenceInteger() else null };
                },
                else => unreachable,
            }
        }
        if (mode == .alter and seen == 0) return self.fail(error.InvalidSqlSyntax, "ALTER SEQUENCE requires an alteration");
        return options;
    }

    fn sequenceDdl(self: *Parser, action: @FieldType(ast.CatalogDdl, "action")) Error!ast.CatalogDdl {
        const conditional = self.keyword(.@"if");
        if (conditional) {
            if (action == .create) try self.expectKeyword(.not);
            try self.expectKeyword(.exists);
        }
        var ddl: ast.CatalogDdl = .{ .kind = .sequence, .action = action, .name = try self.name(), .conditional = conditional };
        if (action == .drop) {
            var targets: std.ArrayList(ast.Name) = .empty;
            try targets.append(self.alloc, ddl.name);
            while (self.take(.comma)) {
                if (targets.items.len >= 256) return self.fail(error.SqlLimitExceeded, "sequence target limit exceeded");
                try targets.append(self.alloc, try self.name());
            }
            ddl.sequence_targets = try targets.toOwnedSlice(self.alloc);
            ddl.cascade = self.keyword(.cascade);
            if (!ddl.cascade) _ = self.keyword(.restrict);
            return ddl;
        }
        if (action == .alter_sequence and self.keyword(.rename)) {
            try self.expectKeyword(.to);
            ddl.action = .rename;
            ddl.new_name = try self.identifier();
            return ddl;
        }
        ddl.sequence_options = try self.sequenceOptions(if (action == .create) .create else .alter);
        return ddl;
    }

    fn catalogDdl(self: *Parser, action: @FieldType(ast.CatalogDdl, "action")) Error!ast.CatalogDdl {
        const kind: @FieldType(ast.CatalogDdl, "kind") = if (self.keyword(.database)) .database else if (self.keyword(.schema)) .namespace else if (self.keyword(.tablespace)) .tablespace else if (self.keyword(.table)) .table else return self.fail(error.UnsupportedSqlShape, "expected a table, database, schema, or tablespace");
        var conditional = false;
        if (action == .create or action == .drop or (action == .rename and kind == .table)) {
            conditional = self.keyword(.@"if");
            if (conditional) {
                if (action == .create) try self.expectKeyword(.not);
                try self.expectKeyword(.exists);
            }
        }
        var target_name: ast.Name = if (kind == .table) (if (action == .rename) try self.tableReferenceName() else try self.name()) else .{ .table = try self.identifier() };
        if (kind == .namespace and self.take(.dot)) {
            target_name.database = target_name.table;
            target_name.table = try self.identifier();
        }
        var ddl: ast.CatalogDdl = .{ .kind = kind, .action = action, .name = target_name, .conditional = conditional };
        if (action == .rename) {
            if (kind == .table and self.keyword(.add)) {
                if (self.keyword(.constraint)) {
                    ddl.action = .alter_schema;
                    ddl.schema_change = try self.tableConstraint();
                    return ddl;
                }
                if (self.peek(.identifier) and (self.tokens[self.pos].isKeyword(.primary) or self.tokens[self.pos].isKeyword(.unique) or self.tokens[self.pos].isKeyword(.check) or self.tokens[self.pos].isKeyword(.foreign))) {
                    ddl.action = .alter_schema;
                    ddl.schema_change = generatedConstraint(try self.constraintDefinition("sql_constraint_0"));
                    return ddl;
                }
                _ = self.keyword(.column);
                const column_name = try self.identifier();
                var column = try self.columnDefinition(column_name);
                var collation_seen = false;
                var null_seen = false;
                while (true) {
                    if (self.keyword(.collate)) {
                        if (collation_seen) return self.fail(error.InvalidSqlSyntax, "duplicate column collation");
                        try self.columnCollation(column);
                        collation_seen = true;
                    } else if (self.keyword(.not)) {
                        if (null_seen) return self.fail(error.InvalidSqlSyntax, "duplicate column nullability");
                        try self.expectKeyword(.null);
                        column.nullable = false;
                        null_seen = true;
                    } else if (self.keyword(.null)) {
                        if (null_seen) return self.fail(error.InvalidSqlSyntax, "duplicate column nullability");
                        null_seen = true;
                    } else if (self.keyword(.default)) {
                        if (column.default_expression != null or column.generated_expression != null or column.sequence != null) return self.fail(error.InvalidSqlSyntax, "duplicate or conflicting column default");
                        column.default_expression = try self.schemaDefault();
                    } else if (self.ddlWord("generated")) {
                        if (column.default_expression != null or column.generated_expression != null or column.sequence != null) return self.fail(error.InvalidSqlSyntax, "duplicate or conflicting generated column");
                        try self.generatedColumn(&column, null_seen and column.nullable);
                    } else if (self.keyword(.constraint)) {
                        if (column.references != null) return self.fail(error.InvalidSqlSyntax, "duplicate column REFERENCES clause");
                        column.references = (try self.foreignKeyDefinition(try self.identifier(), try self.alloc.dupe([]const u8, &.{column_name}))).add_foreign_key;
                    } else if (self.pos < self.tokens.len and !self.tokens[self.pos].owned and std.ascii.eqlIgnoreCase(self.tokens[self.pos].text, "REFERENCES")) {
                        if (column.references != null) return self.fail(error.InvalidSqlSyntax, "duplicate column REFERENCES clause");
                        column.references = generatedConstraint(try self.foreignKeyDefinition("sql_constraint_0", try self.alloc.dupe([]const u8, &.{column_name}))).add_foreign_key;
                    } else break;
                }
                ddl.action = .alter_schema;
                ddl.schema_change = .{ .add_column = column };
            } else if (kind == .table and self.keyword(.drop)) {
                if (self.keyword(.constraint)) {
                    ddl.action = .alter_schema;
                    ddl.schema_change = .{ .drop_constraint = try self.identifier() };
                    return ddl;
                }
                _ = self.keyword(.column);
                ddl.action = .alter_schema;
                ddl.schema_change = .{ .drop_column = try self.identifier() };
            } else if (kind == .table and self.keyword(.alter)) {
                _ = self.keyword(.column);
                const column_name = try self.identifier();
                ddl.action = .alter_schema;
                if (self.ddlWord("TYPE")) {
                    const column = try self.columnDefinition(column_name);
                    if (!self.ddlWord("USING")) return self.fail(error.UnsupportedSqlShape, "ALTER TYPE requires an explicit USING expression");
                    const expression = try self.scalar(0, 0);
                    try self.checkScalarDepth(expression, 0);
                    ddl.schema_change = .{ .set_type = .{ .column = column, .using = expression } };
                } else if (self.keyword(.set)) {
                    if (self.keyword(.not)) {
                        try self.expectKeyword(.null);
                        ddl.schema_change = .{ .set_nullability = .{ .column = column_name, .nullable = false } };
                    } else {
                        try self.expectKeyword(.default);
                        ddl.schema_change = .{ .set_default = .{ .column = column_name, .expression = try self.schemaDefault() } };
                    }
                } else {
                    try self.expectKeyword(.drop);
                    if (self.keyword(.not)) {
                        try self.expectKeyword(.null);
                        ddl.schema_change = .{ .set_nullability = .{ .column = column_name, .nullable = true } };
                    } else {
                        try self.expectKeyword(.default);
                        ddl.schema_change = .{ .drop_default = column_name };
                    }
                }
            } else if (kind == .table and self.ddlWord("VALIDATE")) {
                try self.expectKeyword(.constraint);
                ddl.action = .alter_schema;
                ddl.schema_change = .{ .validate_constraint = try self.identifier() };
            } else if (self.keyword(.rename)) {
                if (kind == .table and self.keyword(.constraint)) {
                    const old_name = try self.identifier();
                    try self.expectKeyword(.to);
                    ddl.action = .alter_schema;
                    ddl.schema_change = .{ .rename_constraint = .{ .name = old_name, .new_name = try self.identifier() } };
                    return ddl;
                }
                if (kind == .table and !(self.pos < self.tokens.len and self.tokens[self.pos].isKeyword(.to))) {
                    _ = self.keyword(.column);
                    const old_name = try self.identifier();
                    try self.expectKeyword(.to);
                    ddl.action = .alter_schema;
                    ddl.schema_change = .{ .rename_column = .{ .name = old_name, .new_name = try self.identifier() } };
                    return ddl;
                }
                try self.expectKeyword(.to);
                ddl.new_name = try self.identifier();
            } else if (self.keyword(.set)) {
                try self.expectKeyword(.tablespace);
                ddl.action = .set_tablespace;
                ddl.tablespace = try self.identifier();
            } else return self.fail(error.UnsupportedSqlShape, "unsupported catalog ALTER operation");
        } else if (action == .create and kind == .tablespace) {
            if (self.keyword(.location)) {
                const location = try self.value();
                if (location != .string) return self.fail(error.InvalidSqlSyntax, "tablespace location must be a string literal");
                ddl.location = location.string;
            }
        } else if (action == .create and self.keyword(.tablespace)) ddl.tablespace = try self.identifier();
        if (conditional and kind == .table and action == .rename and ddl.action != .alter_schema)
            return self.fail(error.UnsupportedSqlShape, "conditional catalog ALTER requires atomic missing-owner admission");
        return ddl;
    }

    fn policyDdl(self: *Parser, action: @FieldType(ast.PolicyDdl, "action")) Error!ast.PolicyDdl {
        var conditional = false;
        if (action == .drop) {
            conditional = self.keyword(.@"if");
            if (conditional) try self.expectKeyword(.exists);
        }
        var ddl: ast.PolicyDdl = .{ .action = action, .name = try self.identifier(), .table = undefined, .if_exists = conditional };
        try self.expectKeyword(.on);
        ddl.table = try self.name();
        if (action == .drop) return ddl;
        if (action == .create and self.keyword(.as)) {
            ddl.permissive = if (self.ddlWord("PERMISSIVE")) true else if (self.ddlWord("RESTRICTIVE")) false else return self.fail(error.InvalidSqlSyntax, "policy AS requires PERMISSIVE or RESTRICTIVE");
        }
        if (action == .create and self.ddlWord("FOR")) {
            ddl.command = if (self.keyword(.all)) .all else if (self.keyword(.select)) .select else if (self.keyword(.insert)) .insert else if (self.keyword(.update)) .update else if (self.keyword(.delete)) .delete else return self.fail(error.InvalidSqlSyntax, "policy FOR requires a supported command");
        }
        if (self.keyword(.to)) {
            ddl.roles_specified = true;
            var roles: std.ArrayList([]const u8) = .empty;
            while (true) {
                if (roles.items.len >= 256) return self.fail(error.SqlLimitExceeded, "too many policy roles");
                try roles.append(self.alloc, if (self.ddlWord("PUBLIC")) "PUBLIC" else try self.identifier());
                if (!self.take(.comma)) break;
            }
            ddl.roles = try roles.toOwnedSlice(self.alloc);
        }
        if (self.ddlWord("USING")) {
            ddl.using_specified = true;
            try self.expect(.lparen);
            ddl.using = try self.scalar(0, 0);
            try self.expect(.rparen);
        }
        if (self.keyword(.with)) {
            if (!self.keyword(.check)) return self.fail(error.InvalidSqlSyntax, "policy WITH requires CHECK");
            ddl.check_specified = true;
            try self.expect(.lparen);
            ddl.with_check = try self.scalar(0, 0);
            try self.expect(.rparen);
        }
        if (action == .create) {
            if (ddl.command == .insert and ddl.using_specified) return self.fail(error.InvalidSqlSyntax, "INSERT policy cannot have USING");
            if ((ddl.command == .select or ddl.command == .delete) and ddl.check_specified) return self.fail(error.InvalidSqlSyntax, "SELECT and DELETE policies cannot have WITH CHECK");
            const truth = try self.alloc.create(ast.Scalar);
            truth.* = .{ .literal = .{ .boolean = true } };
            if (ddl.command != .insert and !ddl.using_specified) ddl.using = truth;
            if ((ddl.command == .all or ddl.command == .insert or ddl.command == .update) and !ddl.check_specified)
                ddl.with_check = ddl.using orelse truth;
        }
        return ddl;
    }

    fn indexDdl(self: *Parser, create: bool, unique: bool) Error!ast.CatalogDdl {
        const concurrently = self.ddlWord("concurrently");
        const conditional = self.keyword(.@"if");
        if (conditional) {
            if (create) try self.expectKeyword(.not);
            try self.expectKeyword(.exists);
        }
        if (!create) {
            var targets = std.ArrayList(ast.Name).empty;
            while (true) {
                try self.node();
                try targets.append(self.alloc, try self.name());
                if (!self.take(.comma)) break;
            }
            // Retain the embedded API's explicit table-owner extension.
            if (self.keyword(.on)) {
                if (concurrently or targets.items.len != 1 or targets.items[0].namespace != null or targets.items[0].database != null) return error.UnsupportedSqlShape;
                const owner = try self.tableReferenceName();
                return .{ .kind = .table, .action = .alter_schema, .name = owner, .conditional = conditional, .schema_change = .{ .drop_index = targets.items[0].table } };
            }
            const cascade = self.keyword(.cascade);
            if (!cascade) _ = self.keyword(.restrict);
            const first = targets.items[0];
            return .{ .kind = .index, .action = .drop, .name = first, .conditional = conditional, .concurrently = concurrently, .cascade = cascade, .index_targets = try targets.toOwnedSlice(self.alloc), .schema_change = .{ .drop_index = first.table } };
        }
        const index_name = try self.identifier();
        try self.expectKeyword(.on);
        const table_name = try self.tableReferenceName();
        try self.expect(.lparen);
        var keys = std.ArrayList(ast.Order).empty;
        while (true) {
            try self.node();
            const expression = try self.scalar(0, 0);
            try self.checkScalarDepth(expression, 0);
            const descending = self.keyword(.desc);
            if (!descending) _ = self.keyword(.asc);
            var nulls_first: ?bool = null;
            if (self.keyword(.nulls)) nulls_first = if (self.keyword(.first)) true else if (self.keyword(.last)) false else return self.fail(error.InvalidSqlSyntax, "expected NULLS FIRST or LAST");
            try keys.append(self.alloc, .{ .field = if (expression.* == .column) expression.column else "", .expression = if (expression.* == .column) null else expression, .descending = descending, .nulls_first = nulls_first });
            if (!self.take(.comma)) break;
        }
        try self.expect(.rparen);
        var included = std.ArrayList([]const u8).empty;
        if (self.keyword(.include)) {
            try self.expect(.lparen);
            while (true) {
                try self.node();
                try included.append(self.alloc, try self.identifier());
                if (!self.take(.comma)) break;
            }
            try self.expect(.rparen);
        }
        const nulls_not_distinct = try self.uniqueNulls();
        if (self.pos < self.tokens.len and (self.tokens[self.pos].isKeyword(.nulls) or self.tokens[self.pos].isKeyword(.include)))
            return self.fail(error.InvalidSqlSyntax, "duplicate or misplaced index NULLS/INCLUDE clause");
        const partial_predicate = if (self.keyword(.where)) try self.scalar(0, 0) else null;
        if (partial_predicate) |expression| try self.checkScalarDepth(expression, 0);
        return .{ .kind = .index, .action = .create, .name = .{ .table = index_name }, .index_table = table_name, .conditional = conditional, .concurrently = concurrently, .schema_change = .{ .create_index = .{ .name = index_name, .keys = try keys.toOwnedSlice(self.alloc), .include_columns = try included.toOwnedSlice(self.alloc), .unique = unique, .predicate = partial_predicate, .nulls_not_distinct = nulls_not_distinct } } };
    }

    fn schemaDefault(self: *Parser) Error!*const ast.Scalar {
        // Persistent defaults cannot capture a request's parameter values or
        // run a subquery. Keep CREATE and both ALTER paths consistent before
        // any catalog admission or physical schema publication.
        var probe = self.pos;
        while (probe < self.tokens.len and self.tokens[probe].kind == .lparen) probe += 1;
        if (probe > self.pos and probe < self.tokens.len and (self.tokens[probe].isKeyword(.select) or self.tokens[probe].isKeyword(.with)))
            return self.fail(error.UnsupportedSqlShape, "subqueries are not allowed in schema defaults");
        const result = try self.scalar(0, 0);
        try self.checkScalarDepth(result, 0);
        try self.persistentExpression(result, false);
        return result;
    }

    fn generatedColumn(self: *Parser, column: *ast.Column, explicit_null: bool) Error!void {
        const start = self.pos;
        const Generation = @import("../system_catalog/column_sequences.zig").Generation;
        const generation: Generation = if (self.ddlWord("always")) .always else by_default: {
            try self.expectKeyword(.by);
            try self.expectKeyword(.default);
            break :by_default .by_default;
        };
        try self.expectKeyword(.as);
        if (!self.ddlWord("identity")) {
            if (generation != .always) return self.fail(error.InvalidSqlSyntax, "BY DEFAULT requires AS IDENTITY");
            self.pos = start;
            column.generated_expression = try self.generatedExpression();
            return;
        }
        if (explicit_null) return self.fail(error.InvalidSqlSyntax, "identity columns cannot declare NULL");
        if (column.type != .integer) return self.fail(error.UnsupportedSqlShape, "identity requires a SMALLINT, INTEGER or BIGINT column");
        var options: ast.SequenceOptions = .{};
        if (self.take(.lparen)) {
            options = try self.sequenceOptions(.identity);
            try self.expect(.rparen);
        }
        var values = options.values;
        values.integer = switch (column.element_type orelse .int64) {
            .int16 => .int16,
            .int32 => .int32,
            .int64 => .int64,
            else => return self.fail(error.UnsupportedSqlShape, "identity requires an integer column"),
        };
        if (options.cache) |cache| values.cache = std.math.cast(u64, cache) orelse return self.fail(error.InvalidSqlSyntax, "CACHE must be positive");
        column.sequence = .{ .column = column.name, .generation = generation, .options = values };
        column.nullable = false;
    }

    fn generatedExpression(self: *Parser) Error!*const ast.Scalar {
        if (!self.ddlWord("always")) return self.fail(error.UnsupportedSqlShape, "only GENERATED ALWAYS AS stored expressions are supported");
        try self.expectKeyword(.as);
        if (self.ddlWord("identity")) return self.fail(error.UnsupportedSqlShape, "identity columns require a durable sequence allocator");
        try self.expect(.lparen);
        const result = try self.scalar(0, 0);
        try self.expect(.rparen);
        if (!self.ddlWord("stored")) return self.fail(error.UnsupportedSqlShape, "virtual generated columns require read-time expression evaluation");
        try self.checkScalarDepth(result, 0);
        try self.persistentExpression(result, true);
        return result;
    }

    // The bounded owned tree, not a token prefix, determines whether durable
    // state could capture request parameters, subqueries or row references.
    fn persistentExpression(self: *Parser, expression: *const ast.Scalar, generated: bool) Error!void {
        switch (expression.*) {
            .literal => |value_| if (value_ == .parameter) return self.fail(error.UnsupportedSqlShape, if (generated) "schema expressions cannot contain execution parameters" else "schema defaults cannot contain execution parameters"),
            .column => if (!generated) return self.fail(error.UnsupportedSqlShape, "column references are not allowed in schema defaults"),
            .unary => |part| try self.persistentExpression(part.operand, generated),
            .binary => |part| {
                try self.persistentExpression(part.left, generated);
                try self.persistentExpression(part.right, generated);
            },
            .cast => |part| try self.persistentExpression(part.operand, generated),
            .call => |part| {
                if (part.subquery != null or part.star or part.distinct or part.filter != null or part.window != null or part.within_group != null)
                    return self.fail(error.UnsupportedSqlShape, "subqueries, aggregates and windows are not allowed in schema expressions");
                for (part.args) |arg| try self.persistentExpression(arg, generated);
            },
            .case_when => |part| {
                for (part.branches) |branch| {
                    try self.persistentExpression(branch.condition, generated);
                    try self.persistentExpression(branch.value, generated);
                }
                if (part.otherwise) |other| try self.persistentExpression(other, generated);
            },
            .in_list => |part| {
                try self.persistentExpression(part.operand, generated);
                for (part.values) |value_| try self.persistentExpression(value_, generated);
            },
        }
    }

    fn ddlWord(self: *Parser, word: []const u8) bool {
        if (self.pos >= self.tokens.len or self.tokens[self.pos].owned or !std.ascii.eqlIgnoreCase(self.tokens[self.pos].text, word)) return false;
        self.pos += 1;
        return true;
    }

    /// Definition admission is deliberately separate from routine activation.
    /// Validate the supported declaration grammar to its end before reporting
    /// a missing body. Never publish a definition or execute a valid prefix.
    fn routineAdmission(self: *Parser, procedure: bool) Error!ast.Statement {
        _ = try self.name();
        try self.expect(.lparen);
        // Parameterized routine activation requires catalog-bound signatures.
        // Do not guess type resolution from the spelling of an argument.
        if (!self.take(.rparen)) return self.fail(error.UnsupportedSqlShape, "catalog-bound routine signatures are not supported");
        if (!procedure) {
            if (!self.ddlWord("RETURNS")) return self.fail(error.InvalidSqlSyntax, "a function requires RETURNS");
            if (!self.ddlWord("TRIGGER") and !self.ddlWord("VOID")) return self.fail(error.UnsupportedSqlShape, "catalog-bound routine return types are not supported");
        }
        var language: ?[]const u8 = null;
        var body = false;
        var seen: u16 = 0;
        while (self.pos < self.tokens.len and !self.peek(.semicolon)) {
            var option: u4 = undefined;
            if (self.ddlWord("LANGUAGE")) {
                option = 0;
                language = try self.identifier();
            } else if (self.ddlWord("AS")) {
                option = 1;
                try self.expect(.string);
                if (self.take(.comma)) try self.expect(.string);
                body = true;
            } else if (self.ddlWord("IMMUTABLE") or self.ddlWord("STABLE") or self.ddlWord("VOLATILE")) {
                option = 2;
            } else if (self.ddlWord("EXTERNAL") or self.ddlWord("SECURITY")) {
                option = 3;
                if (std.ascii.eqlIgnoreCase(self.tokens[self.pos - 1].text, "EXTERNAL")) {
                    if (!self.ddlWord("SECURITY")) return self.fail(error.InvalidSqlSyntax, "EXTERNAL requires SECURITY");
                }
                if (!self.ddlWord("DEFINER") and !self.ddlWord("INVOKER")) return self.fail(error.InvalidSqlSyntax, "SECURITY requires DEFINER or INVOKER");
            } else if (self.ddlWord("STRICT") or self.ddlWord("CALLED") or self.ddlWord("RETURNS")) {
                option = 4;
                if (std.ascii.eqlIgnoreCase(self.tokens[self.pos - 1].text, "CALLED")) {
                    if (!self.ddlWord("ON")) return self.fail(error.InvalidSqlSyntax, "CALLED requires ON NULL INPUT");
                    if (!self.ddlWord("NULL") or !self.ddlWord("INPUT")) return self.fail(error.InvalidSqlSyntax, "CALLED requires ON NULL INPUT");
                } else if (std.ascii.eqlIgnoreCase(self.tokens[self.pos - 1].text, "RETURNS")) {
                    if (!self.ddlWord("NULL") or !self.ddlWord("ON") or !self.ddlWord("NULL") or !self.ddlWord("INPUT")) return self.fail(error.InvalidSqlSyntax, "RETURNS requires NULL ON NULL INPUT");
                }
            } else if (self.ddlWord("PARALLEL")) {
                option = 5;
                if (!self.ddlWord("SAFE") and !self.ddlWord("RESTRICTED") and !self.ddlWord("UNSAFE")) return self.fail(error.InvalidSqlSyntax, "invalid PARALLEL option");
            } else if (self.ddlWord("COST") or self.ddlWord("ROWS")) {
                option = if (std.ascii.eqlIgnoreCase(self.tokens[self.pos - 1].text, "COST")) 6 else 7;
                try self.expect(.number);
            } else if (self.ddlWord("WINDOW")) {
                option = 8;
            } else if (self.ddlWord("LEAKPROOF") or self.ddlWord("NOT")) {
                option = 9;
                if (self.tokens[self.pos - 1].isKeyword(.not) and !self.ddlWord("LEAKPROOF")) return self.fail(error.InvalidSqlSyntax, "NOT requires LEAKPROOF");
            } else if (self.ddlWord("SUPPORT")) {
                return self.fail(error.UnsupportedSqlShape, "SUPPORT requires catalog-bound routine lookup");
            } else if (self.ddlWord("TRANSFORM")) {
                option = 11;
                while (true) {
                    if (!self.ddlWord("FOR") or !self.ddlWord("TYPE")) return self.fail(error.InvalidSqlSyntax, "each TRANSFORM entry requires FOR TYPE");
                    _ = try self.name();
                    if (!self.take(.comma)) break;
                }
            } else if (self.ddlWord("SET")) {
                _ = try self.name();
                if (self.ddlWord("FROM")) {
                    if (!self.ddlWord("CURRENT")) return self.fail(error.InvalidSqlSyntax, "SET FROM requires CURRENT");
                } else {
                    if (!self.ddlWord("TO") and !self.take(.eq)) return self.fail(error.InvalidSqlSyntax, "SET requires TO or equals");
                    while (true) {
                        if (!self.take(.string) and !self.take(.number)) _ = try self.identifier();
                        if (!self.take(.comma)) break;
                    }
                }
                continue;
            } else return self.fail(error.UnsupportedSqlShape, "routine option is not supported");
            const bit = @as(u16, 1) << option;
            if (seen & bit != 0) return self.fail(error.InvalidSqlSyntax, "conflicting or redundant routine options");
            seen |= bit;
        }
        _ = self.take(.semicolon);
        if (self.pos != self.tokens.len) return self.fail(error.InvalidSqlSyntax, "submit one routine definition");
        // Language lookup and validation must precede body validation. Only a
        // built-in supported language can produce this definitive diagnostic.
        if (language) |name_| if (std.mem.eql(u8, name_, "plpgsql") or std.mem.eql(u8, name_, "sql")) {
            if (!body) return self.fail(error.SqlInvalidFunctionDefinition, "no function body specified");
        };
        return self.fail(error.UnsupportedSqlShape, "routine catalog activation is not supported");
    }

    const UniqueColumnList = struct { columns: []const []const u8, without_overlaps_column: ?[]const u8 = null };

    fn ddlColumnList(self: *Parser) Error![]const []const u8 {
        return (try self.ddlColumnListMode(false)).columns;
    }

    fn ddlUniqueColumnList(self: *Parser) Error!UniqueColumnList {
        return self.ddlColumnListMode(true);
    }

    fn ddlColumnListMode(self: *Parser, temporal_unique: bool) Error!UniqueColumnList {
        try self.expect(.lparen);
        var columns = std.ArrayList([]const u8).empty;
        var period: ?[]const u8 = null;
        while (true) {
            try self.node();
            const column = try self.identifier();
            try columns.append(self.alloc, column);
            if (temporal_unique and self.keyword(.without)) {
                try self.expectKeyword(.overlaps);
                period = column;
                break;
            }
            if (!self.take(.comma)) break;
        }
        try self.expect(.rparen);
        return .{ .columns = try columns.toOwnedSlice(self.alloc), .without_overlaps_column = period };
    }

    fn foreignKeyAction(self: *Parser) Error![]const u8 {
        if (self.keyword(.cascade)) return "cascade";
        if (self.keyword(.restrict)) return "restrict";
        if (self.keyword(.set)) {
            try self.expectKeyword(.null);
            return "set_null";
        }
        if (self.ddlWord("NO") and self.ddlWord("ACTION")) return "no_action";
        return self.fail(error.UnsupportedSqlShape, "unsupported foreign key action");
    }

    fn generatedConstraint(change: ast.SchemaChange) ast.SchemaChange {
        var result = change;
        switch (result) {
            .add_unique => |*rule| rule.name_generated = true,
            .add_check => |*rule| rule.name_generated = true,
            .add_foreign_key => |*rule| rule.name_generated = true,
            else => unreachable,
        }
        return result;
    }

    fn tableConstraint(self: *Parser) Error!ast.SchemaChange {
        const constraint_name = try self.identifier();
        return self.constraintDefinition(constraint_name);
    }

    fn uniqueNulls(self: *Parser) Error!bool {
        if (!self.keyword(.nulls)) return false;
        const not_distinct = self.keyword(.not);
        try self.expectKeyword(.distinct);
        return not_distinct;
    }

    fn uniqueTiming(self: *Parser, change: ast.SchemaChange) Error!ast.SchemaChange {
        var result = change;
        if (self.ddlWord("INCLUDE")) result.add_unique.include_columns = try self.ddlColumnList();
        var timing_seen = false;
        var deferrable_seen = false;
        while (true) {
            if (self.keyword(.deferrable)) {
                if (deferrable_seen) return self.fail(error.InvalidSqlSyntax, "duplicate DEFERRABLE clause");
                deferrable_seen = true;
                result.add_unique.deferrable = true;
            } else if (self.pos + 1 < self.tokens.len and self.tokens[self.pos].isKeyword(.not) and self.tokens[self.pos + 1].isKeyword(.deferrable)) {
                if (deferrable_seen) return self.fail(error.InvalidSqlSyntax, "duplicate DEFERRABLE clause");
                deferrable_seen = true;
                self.pos += 2;
            } else if (self.ddlWord("INITIALLY")) {
                if (timing_seen) return self.fail(error.InvalidSqlSyntax, "duplicate INITIALLY clause");
                timing_seen = true;
                result.add_unique.timing = if (self.keyword(.deferred)) "deferred" else if (self.keyword(.immediate)) "immediate" else return self.fail(error.InvalidSqlSyntax, "invalid constraint timing");
            } else break;
        }
        if (std.mem.eql(u8, result.add_unique.timing, "deferred") and !result.add_unique.deferrable) return self.fail(error.InvalidSqlSyntax, "INITIALLY DEFERRED requires DEFERRABLE");
        if (self.pos < self.tokens.len and self.tokens[self.pos].isKeyword(.nulls)) return self.fail(error.InvalidSqlSyntax, "constraint NULLS clause must follow UNIQUE");
        return result;
    }

    fn inlineConstraint(self: *Parser, constraint_name: []const u8, column: []const u8) Error!ast.SchemaChange {
        const columns = try self.alloc.dupe([]const u8, &.{column});
        if (self.keyword(.primary)) {
            try self.expectKeyword(.key);
            return self.uniqueTiming(.{ .add_unique = .{ .name = constraint_name, .columns = columns, .primary = true } });
        }
        if (self.keyword(.unique)) {
            const nulls_not_distinct = try self.uniqueNulls();
            return self.uniqueTiming(.{ .add_unique = .{ .name = constraint_name, .columns = columns, .nulls_not_distinct = nulls_not_distinct } });
        }
        if (self.peek(.identifier) and self.tokens[self.pos].isKeyword(.check)) return self.constraintDefinition(constraint_name);
        return self.foreignKeyDefinition(constraint_name, columns);
    }

    fn constraintDefinition(self: *Parser, constraint_name: []const u8) Error!ast.SchemaChange {
        if (self.keyword(.primary)) {
            try self.expectKeyword(.key);
            const list = try self.ddlUniqueColumnList();
            return self.uniqueTiming(.{ .add_unique = .{ .name = constraint_name, .columns = list.columns, .without_overlaps_column = list.without_overlaps_column, .primary = true } });
        }
        if (self.keyword(.unique)) {
            const nulls_not_distinct = try self.uniqueNulls();
            const list = try self.ddlUniqueColumnList();
            return self.uniqueTiming(.{ .add_unique = .{ .name = constraint_name, .columns = list.columns, .without_overlaps_column = list.without_overlaps_column, .nulls_not_distinct = nulls_not_distinct } });
        }
        if (self.keyword(.check)) {
            try self.expect(.lparen);
            const expression = try self.scalar(0, 0);
            try self.checkScalarDepth(expression, 0);
            try self.expect(.rparen);
            const not_valid = self.keyword(.not);
            if (not_valid and !self.ddlWord("VALID")) return self.fail(error.InvalidSqlSyntax, "expected NOT VALID after CHECK");
            return .{ .add_check = .{ .name = constraint_name, .expression = expression, .not_valid = not_valid } };
        }
        try self.expectKeyword(.foreign);
        try self.expectKeyword(.key);
        const columns = try self.ddlColumnList();
        return self.foreignKeyDefinition(constraint_name, columns);
    }

    fn foreignKeyDefinition(self: *Parser, constraint_name: []const u8, columns: []const []const u8) Error!ast.SchemaChange {
        if (!self.ddlWord("REFERENCES")) return self.fail(error.InvalidSqlSyntax, "expected REFERENCES");
        const parent = try self.identifier();
        var foreign: @FieldType(ast.SchemaChange, "add_foreign_key") = .{ .name = constraint_name, .columns = columns, .parent = parent, .parent_columns = try self.ddlColumnList() };
        var on_delete = false;
        var on_update = false;
        var match_seen = false;
        var deferrable_seen = false;
        var timing_seen = false;
        while (true) {
            if (self.keyword(.on)) {
                if (self.keyword(.delete)) {
                    if (on_delete) return self.fail(error.InvalidSqlSyntax, "duplicate ON DELETE action");
                    on_delete = true;
                    foreign.on_delete = try self.foreignKeyAction();
                } else {
                    try self.expectKeyword(.update);
                    if (on_update) return self.fail(error.InvalidSqlSyntax, "duplicate ON UPDATE action");
                    on_update = true;
                    foreign.on_update = try self.foreignKeyAction();
                }
            } else if (self.ddlWord("MATCH")) {
                if (match_seen) return self.fail(error.InvalidSqlSyntax, "duplicate MATCH clause");
                match_seen = true;
                foreign.match = if (self.ddlWord("SIMPLE")) "simple" else if (self.ddlWord("FULL")) "full" else if (self.ddlWord("PARTIAL")) "partial" else return self.fail(error.InvalidSqlSyntax, "invalid MATCH clause");
            } else if (self.keyword(.deferrable)) {
                if (deferrable_seen) return self.fail(error.InvalidSqlSyntax, "duplicate DEFERRABLE clause");
                deferrable_seen = true;
                foreign.deferrable = true;
            } else if (self.ddlWord("INITIALLY")) {
                if (timing_seen) return self.fail(error.InvalidSqlSyntax, "duplicate INITIALLY clause");
                timing_seen = true;
                foreign.timing = if (self.keyword(.deferred)) "deferred" else if (self.keyword(.immediate)) "immediate" else return self.fail(error.InvalidSqlSyntax, "invalid constraint timing");
            } else break;
        }
        if (self.keyword(.not)) {
            if (!self.ddlWord("VALID")) return self.fail(error.InvalidSqlSyntax, "expected NOT VALID after foreign key");
            foreign.not_valid = true;
        }
        if (std.mem.eql(u8, foreign.timing, "deferred") and !foreign.deferrable) return self.fail(error.InvalidSqlSyntax, "INITIALLY DEFERRED requires DEFERRABLE");
        return .{ .add_foreign_key = foreign };
    }

    fn copyString(self: *Parser) Error![]const u8 {
        const value_ = try self.value();
        if (value_ != .string) return self.fail(error.InvalidSqlSyntax, "COPY option requires a string");
        return value_.string;
    }

    fn copyByte(self: *Parser) Error!u8 {
        const text = try self.copyString();
        if (text.len != 1 or text[0] == 0 or text[0] >= 128) return self.fail(error.SqlInvalidParameterValue, "COPY option requires one ASCII byte");
        return text[0];
    }

    fn copyTo(self: *Parser) Error!ast.Statement {
        self.relation_depth += 1;
        defer self.relation_depth -= 1;
        if (self.relation_depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "COPY nesting limit exceeded");
        const query = try self.alloc.create(ast.Select);
        var implicit_columns = false;
        const query_copy = self.peek(.lparen);
        if (self.take(.lparen)) {
            const nested = try self.statement();
            if (nested != .select) return self.fail(error.UnsupportedSqlShape, "COPY query must be a read-only SELECT");
            query.* = nested.select;
            try self.expect(.rparen);
        } else {
            const table_name = try self.tableReferenceName();
            var projections: std.ArrayList(ast.Projection) = .empty;
            var column_names: std.StringHashMapUnmanaged(void) = .empty;
            if (self.peek(.lparen)) for (try self.ddlColumnList()) |column| {
                const entry = try column_names.getOrPut(self.alloc, column);
                if (entry.found_existing) return self.fail(error.DuplicateSqlColumn, "duplicate COPY column");
                try projections.append(self.alloc, .{ .field = column });
            };
            implicit_columns = projections.items.len == 0;
            query.* = .{ .table = table_name, .columns = try projections.toOwnedSlice(self.alloc) };
        }
        const input = self.ddlWord("FROM");
        if (!input and !self.ddlWord("TO")) return self.fail(error.InvalidSqlSyntax, "COPY requires TO or FROM");
        if (input and query_copy) return self.fail(error.InvalidSqlSyntax, "COPY FROM requires a table");
        // PostgreSQL's grammar accepts either stream spelling in either
        // direction; TO determines CopyOut even when the token says STDIN.
        if (!self.ddlWord("STDOUT") and !self.ddlWord("STDIN")) return self.fail(error.UnsupportedSqlShape, "server files and PROGRAM are not COPY client streams");
        var options: ast.CopyOptions = .{};
        var on_error_seen = false;
        const with_options = self.keyword(.with);
        if (with_options and !self.peek(.lparen)) return self.fail(error.InvalidSqlSyntax, "COPY WITH requires options");
        if (self.take(.lparen)) {
            const Option = enum { format, delimiter, null, header, quote, escape, force_quote, encoding, force_null, force_not_null, default, on_error, reject_limit, log_verbosity, oids };
            var seen: u16 = 0;
            while (true) {
                try self.node();
                if (!self.peek(.identifier)) return self.fail(error.InvalidSqlSyntax, "expected COPY option");
                const option_token = self.tokens[self.pos];
                self.pos += 1;
                const folded = try self.alloc.dupe(u8, option_token.text);
                if (!option_token.owned) _ = std.ascii.lowerString(folded, folded);
                const option = std.meta.stringToEnum(Option, folded) orelse return self.fail(error.UnsupportedSqlShape, "unsupported COPY option");
                const mask = @as(u16, 1) << @intCast(@backingInt(option));
                if (seen & mask != 0) return self.fail(error.SqlInvalidParameterValue, "duplicate COPY option");
                seen |= mask;
                switch (option) {
                    .format => {
                        const format_text = if (self.peek(.string)) try self.copyString() else try self.identifier();
                        options.format = std.meta.stringToEnum(@FieldType(ast.CopyOptions, "format"), format_text) orelse return self.fail(error.SqlInvalidParameterValue, "unknown COPY format");
                    },
                    .delimiter => options.delimiter = try self.copyByte(),
                    .null => options.null_string = try self.copyString(),
                    .quote => options.quote = try self.copyByte(),
                    .escape => options.escape = try self.copyByte(),
                    .header => {
                        if (self.pos == self.tokens.len) return self.fail(error.InvalidSqlSyntax, "COPY HEADER requires a closing option list");
                        if (self.peek(.comma) or self.peek(.rparen)) {
                            options.header = true;
                        } else {
                            const boolean = self.tokens[self.pos].text;
                            self.pos += 1;
                            if (std.ascii.eqlIgnoreCase(boolean, "match")) {
                                options.header = true;
                                options.header_match = true;
                            } else if (std.ascii.eqlIgnoreCase(boolean, "true") or std.ascii.eqlIgnoreCase(boolean, "on") or std.mem.eql(u8, boolean, "1")) options.header = true else if (std.ascii.eqlIgnoreCase(boolean, "false") or std.ascii.eqlIgnoreCase(boolean, "off") or std.mem.eql(u8, boolean, "0")) options.header = false else return self.fail(error.SqlInvalidParameterValue, "COPY HEADER requires a boolean");
                        }
                    },
                    .encoding => {
                        const text = try self.copyString();
                        if (!std.ascii.eqlIgnoreCase(text, "UTF8") and !std.ascii.eqlIgnoreCase(text, "UTF-8")) return self.fail(error.UnsupportedSqlShape, "COPY encoding must be UTF8");
                    },
                    .oids => return self.fail(error.InvalidSqlSyntax, "option oids not recognized"),
                    .default => options.default_string = try self.copyString(),
                    .on_error => {
                        const option_value = if (self.peek(.string)) try self.copyString() else try self.identifier();
                        on_error_seen = true;
                        options.on_error = if (std.ascii.eqlIgnoreCase(option_value, "ignore")) .ignore else if (std.ascii.eqlIgnoreCase(option_value, "stop")) .stop else return self.fail(error.SqlInvalidParameterValue, "COPY ON_ERROR requires STOP or IGNORE");
                    },
                    .log_verbosity => {
                        const option_value = if (self.peek(.string)) try self.copyString() else try self.identifier();
                        options.log_verbosity = if (std.ascii.eqlIgnoreCase(option_value, "default")) .default else if (std.ascii.eqlIgnoreCase(option_value, "verbose")) .verbose else if (std.ascii.eqlIgnoreCase(option_value, "silent")) .silent else return self.fail(error.SqlInvalidParameterValue, "invalid COPY LOG_VERBOSITY");
                    },
                    .reject_limit => {
                        const option_value = try self.value();
                        const limit = if (option_value == .integer) option_value.integer else if (option_value == .string) std.fmt.parseInt(i64, option_value.string, 10) catch return self.fail(error.SqlInvalidParameterValue, "invalid COPY REJECT_LIMIT") else return self.fail(error.SqlInvalidParameterValue, "COPY REJECT_LIMIT must be positive");
                        if (limit <= 0) return self.fail(error.SqlInvalidParameterValue, "COPY REJECT_LIMIT must be positive");
                        options.reject_limit = @intCast(limit);
                    },
                    .force_null, .force_not_null => {
                        const all = self.take(.star);
                        const names = if (all) &.{} else try self.ddlColumnList();
                        var unique: std.StringHashMapUnmanaged(void) = .empty;
                        for (names) |column_name| if ((try unique.getOrPut(self.alloc, column_name)).found_existing) return self.fail(error.DuplicateSqlColumn, "duplicate COPY force column");
                        if (option == .force_null) {
                            options.force_null_all = all;
                            options.force_null_columns = names;
                        } else {
                            options.force_not_null_all = all;
                            options.force_not_null_columns = names;
                        }
                    },
                    .force_quote => {
                        if (self.take(.star)) options.force_quote_all = true else options.force_quote_columns = try self.ddlColumnList();
                        var names: std.StringHashMapUnmanaged(void) = .empty;
                        for (options.force_quote_columns) |column| {
                            const entry = try names.getOrPut(self.alloc, column);
                            if (entry.found_existing) return self.fail(error.DuplicateSqlColumn, "duplicate FORCE_QUOTE column");
                        }
                    },
                }
                if (!self.take(.comma)) break;
            }
            try self.expect(.rparen);
        }
        options.validate() catch |err| return self.fail(switch (err) {
            error.InvalidSqlSyntax => error.InvalidSqlSyntax,
            error.UnsupportedSqlShape => error.UnsupportedSqlShape,
            else => error.SqlInvalidParameterValue,
        }, "invalid COPY format options");
        if (!input) {
            if (options.header_match or options.default_string != null) return self.fail(error.UnsupportedSqlShape, "COPY input option is not allowed with COPY TO");
            if (on_error_seen or options.force_null_all or options.force_not_null_all or options.force_null_columns.len != 0 or options.force_not_null_columns.len != 0 or options.reject_limit != null) return self.fail(error.SqlInvalidParameterValue, "COPY input option is not allowed with COPY TO");
        }
        if (input) {
            if (options.force_quote_all or options.force_quote_columns.len != 0) return self.fail(error.UnsupportedSqlShape, "FORCE_QUOTE is only valid for COPY TO");
            const columns = try self.alloc.alloc([]const u8, query.columns.len);
            for (columns, query.columns) |*out, projection| out.* = projection.field;
            return .{ .copy_from = .{ .table = query.table.?, .columns = columns, .options = options } };
        }
        return .{ .copy_to = .{ .query = query, .options = options, .implicit_columns = implicit_columns } };
    }

    fn explainBoolean(self: *Parser) Error!bool {
        if (self.keyword(.on) or self.keyword(.true)) return true;
        if (self.keyword(.off) or self.keyword(.false)) return false;
        if (self.peek(.number) or self.peek(.string)) {
            const literal = try self.value();
            switch (literal) {
                .integer => |n| if (n == 0 or n == 1) return n != 0,
                .string => |text| {
                    inline for (.{ "true", "on" }) |word| if (std.ascii.eqlIgnoreCase(text, word)) return true;
                    inline for (.{ "false", "off" }) |word| if (std.ascii.eqlIgnoreCase(text, word)) return false;
                },
                else => {},
            }
            return self.fail(error.InvalidSqlSyntax, "EXPLAIN option requires a Boolean value");
        }
        return true;
    }

    fn statement(self: *Parser) Error!ast.Statement {
        try self.node();
        if (self.ddlWord("COPY")) return self.copyTo();
        if (self.keyword(.explain)) {
            const inner = try self.alloc.create(ast.Statement);
            var options: ast.Explain = .{ .statement = inner, .analyze = self.keyword(.analyze) };
            options.verbose = self.keyword(.verbose);
            if (self.take(.lparen)) {
                while (true) {
                    if (self.keyword(.format)) {
                        if (self.keyword(.json)) {
                            options.format = .json;
                        } else if (self.ddlWord("TEXT")) {
                            options.format = .text;
                        } else {
                            const format_name = if (self.peek(.string)) (try self.value()).string else try self.identifier();
                            if (std.ascii.eqlIgnoreCase(format_name, "json")) options.format = .json else if (std.ascii.eqlIgnoreCase(format_name, "text")) options.format = .text else if (std.ascii.eqlIgnoreCase(format_name, "xml") or std.ascii.eqlIgnoreCase(format_name, "yaml")) return self.fail(error.UnsupportedSqlShape, "EXPLAIN supports TEXT and JSON formats") else return self.fail(error.SqlInvalidParameterValue, "unrecognized EXPLAIN format");
                        }
                    } else if (self.keyword(.verbose)) {
                        options.verbose = try self.explainBoolean();
                    } else if (self.keyword(.analyze)) {
                        options.analyze = try self.explainBoolean();
                    } else if (self.keyword(.costs)) {
                        options.costs = try self.explainBoolean();
                    } else if (self.ddlWord("BUFFERS")) {
                        options.buffers = try self.explainBoolean();
                    } else if (self.ddlWord("TIMING")) {
                        options.timing = try self.explainBoolean();
                    } else if (self.ddlWord("SUMMARY")) {
                        options.summary = try self.explainBoolean();
                    } else if (self.ddlWord("SETTINGS")) {
                        options.settings = try self.explainBoolean();
                    } else if (self.ddlWord("WAL")) {
                        options.wal = try self.explainBoolean();
                    } else return self.fail(error.InvalidSqlSyntax, "unrecognized EXPLAIN option");
                    if (!self.take(.comma)) break;
                }
                try self.expect(.rparen);
            }
            try options.validate();
            inner.* = try self.statement();
            switch (inner.*) {
                .select, .insert, .update, .delete, .merge => {},
                else => return self.fail(error.UnsupportedSqlShape, "EXPLAIN requires a supported query or mutation"),
            }
            return .{ .explain = options };
        }
        if (self.keyword(.truncate)) {
            _ = self.keyword(.table);
            var tables: std.ArrayList(ast.Name) = .empty;
            while (true) {
                try tables.append(self.alloc, try self.tableReferenceName());
                if (tables.items.len > 128) return self.fail(error.SqlLimitExceeded, "TRUNCATE table limit exceeded");
                if (!self.take(.comma)) break;
            }
            var restart = false;
            if (self.keyword(.restart)) {
                try self.expectKeyword(.identity);
                restart = true;
            } else if (self.keyword(.@"continue")) try self.expectKeyword(.identity);
            const cascade = self.keyword(.cascade);
            if (!cascade) _ = self.keyword(.restrict);
            return .{ .catalog_ddl = .{ .kind = .table, .action = .truncate, .name = tables.items[0], .truncate_tables = try tables.toOwnedSlice(self.alloc), .restart_identity = restart, .cascade = cascade } };
        }
        if (self.keyword(.with)) {
            const recursive = self.keyword(.recursive);
            self.relation_depth += 1;
            defer self.relation_depth -= 1;
            if (self.relation_depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL CTE nesting limit exceeded");
            var ctes: std.ArrayList(ast.Cte) = .empty;
            while (true) {
                const cte_name = try self.identifier();
                var column_names: std.ArrayList([]const u8) = .empty;
                if (self.take(.lparen)) {
                    while (true) {
                        try column_names.append(self.alloc, try self.identifier());
                        if (!self.take(.comma)) break;
                    }
                    try self.expect(.rparen);
                }
                try self.expectKeyword(.as);
                const materialization: ast.Cte.Materialization = if (self.keyword(.materialized)) .materialized else if (self.keyword(.not)) blk: {
                    try self.expectKeyword(.materialized);
                    break :blk .not_materialized;
                } else .automatic;
                try self.expect(.lparen);
                const cte_statement = try self.statement();
                if (cte_statement != .select) return self.fail(error.UnsupportedSqlShape, "CTEs require SELECT queries");
                try self.expect(.rparen);
                const query = try self.alloc.create(ast.Select);
                query.* = cte_statement.select;
                try ctes.append(self.alloc, .{ .name = cte_name, .columns = try column_names.toOwnedSlice(self.alloc), .query = query, .recursive = recursive, .materialization = materialization });
                if (!self.take(.comma)) break;
            }
            var result = try self.statement();
            switch (result) {
                .update => |*mutation| {
                    try ctes.appendSlice(self.alloc, mutation.ctes);
                    mutation.ctes = try ctes.toOwnedSlice(self.alloc);
                    if (mutation.source == null) mutation.source = try self.relationNode(.{ .table = .{ .name = mutation.table, .alias = mutation.alias, .mutation_target = true, .mutation_document = true, .mutation_presence = true } });
                    return result;
                },
                .delete => |*mutation| {
                    mutation.ctes = try ctes.toOwnedSlice(self.alloc);
                    if (mutation.source == null) mutation.source = try self.relationNode(.{ .table = .{ .name = mutation.table, .alias = mutation.alias, .mutation_target = true, .mutation_presence = true } });
                    return result;
                },
                .merge => |*mutation| {
                    if (recursive) return self.fail(error.InvalidSqlSyntax, "WITH RECURSIVE is not supported for MERGE");
                    mutation.ctes = try ctes.toOwnedSlice(self.alloc);
                    return result;
                },
                else => {},
            }
            const source: *ast.Select = switch (result) {
                .select => &result.select,
                .insert => blk: {
                    const original = result.insert.source orelse return self.fail(error.UnsupportedSqlShape, "WITH INSERT requires a SELECT source");
                    const copy = try self.alloc.create(ast.Select);
                    copy.* = original.*;
                    result.insert.source = copy;
                    break :blk copy;
                },
                else => return self.fail(error.UnsupportedSqlShape, "WITH requires a query or mutation"),
            };
            try ctes.appendSlice(self.alloc, source.ctes);
            source.ctes = try ctes.toOwnedSlice(self.alloc);
            return result;
        }
        if (self.take(.lparen)) {
            self.relation_depth += 1;
            defer self.relation_depth -= 1;
            if (self.relation_depth >= self.limits.max_depth) return self.fail(error.SqlLimitExceeded, "SQL query nesting limit exceeded");
            const nested = try self.statement();
            if (nested != .select) return self.fail(error.InvalidSqlSyntax, "parenthesized queries require SELECT");
            try self.expect(.rparen);
            const next_kind = if (self.pos < self.tokens.len) self.tokens[self.pos] else null;
            if (next_kind != null and (next_kind.?.isKeyword(.@"union") or next_kind.?.isKeyword(.intersect) or next_kind.?.isKeyword(.except)))
                return .{ .select = try self.selectFinish(nested.select) };
            if (next_kind == null or (!next_kind.?.isKeyword(.order) and !next_kind.?.isKeyword(.limit) and !next_kind.?.isKeyword(.offset))) return nested;
            // Preserve inner ORDER/LIMIT: outer clauses apply to a derived
            // relation, never overwrite the parenthesized query's clauses.
            const query = try self.alloc.create(ast.Select);
            query.* = nested.select;
            const source = try self.alloc.create(ast.Relation);
            source.* = .{ .derived = .{ .query = query, .alias = "$parenthesized" } };
            return .{ .select = try self.selectFinish(.{ .source = source }) };
        }
        if (self.keyword(.select)) return .{ .select = try self.select() };
        if (self.keyword(.values)) return .{ .select = try self.selectFinish(try self.valuesCore()) };
        if (self.keyword(.insert)) return .{ .insert = try self.insert() };
        if (self.keyword(.update)) return .{ .update = try self.update() };
        if (self.keyword(.delete)) {
            return .{ .delete = try self.delete() };
        }
        if (self.keyword(.merge)) return .{ .merge = try self.merge() };
        if (self.ddlWord("COMMENT")) {
            try self.expectKeyword(.on);
            const column_comment = self.keyword(.column);
            const kind: @FieldType(ast.CatalogDdl, "kind") = if (column_comment or self.keyword(.table)) .table else if (self.keyword(.schema)) .namespace else if (self.keyword(.database)) .database else return self.fail(error.UnsupportedSqlShape, "this comment target requires catalog annotation support");
            var comment_column: ?[]const u8 = null;
            const target = if (column_comment) column_target: {
                var parts: [4][]const u8 = undefined;
                var count: usize = 0;
                while (true) {
                    if (count == parts.len) return self.fail(error.InvalidSqlSyntax, "column comment name has too many qualifiers");
                    parts[count] = try self.identifier();
                    count += 1;
                    if (!self.take(.dot)) break;
                }
                if (count < 2) return self.fail(error.InvalidSqlSyntax, "column comment requires a table and column");
                comment_column = parts[count - 1];
                break :column_target ast.Name{ .database = if (count == 4) parts[0] else null, .namespace = if (count >= 3) parts[count - 3] else null, .table = parts[count - 2] };
            } else try self.name();
            if (kind != .table and (target.namespace != null or target.database != null))
                return self.fail(error.InvalidSqlSyntax, "schema and database comments require a single identifier");
            try self.expectKeyword(.is);
            if (!self.peek(.string) and !(self.pos < self.tokens.len and self.tokens[self.pos].isKeyword(.null)))
                return self.fail(error.InvalidSqlSyntax, "COMMENT requires a string literal or NULL");
            const text = try self.value();
            return .{ .catalog_ddl = .{ .kind = kind, .action = .comment, .name = target, .comment_value = if (text == .string) text.string else null, .comment_column = comment_column } };
        }
        if (self.keyword(.create)) {
            if (self.keyword(.@"or")) {
                if (!self.ddlWord("REPLACE")) return self.fail(error.InvalidSqlSyntax, "CREATE OR requires REPLACE");
                if (self.pos < self.tokens.len and (self.tokens[self.pos].isKeyword(.table) or self.tokens[self.pos].isKeyword(.materialized)))
                    return self.fail(error.InvalidSqlSyntax, "PostgreSQL does not permit CREATE OR REPLACE TABLE or MATERIALIZED VIEW");
                if (self.ddlWord("FUNCTION")) return self.routineAdmission(false);
                if (self.ddlWord("PROCEDURE")) return self.routineAdmission(true);
                if (!self.ddlWord("VIEW")) return self.fail(error.UnsupportedSqlShape, "this CREATE OR REPLACE object is not supported");
                return .{ .catalog_ddl = try self.viewDdl(.create, true) };
            }
            if (self.ddlWord("FUNCTION")) return self.routineAdmission(false);
            if (self.ddlWord("PROCEDURE")) return self.routineAdmission(true);
            if (self.ddlWord("SEQUENCE")) return .{ .catalog_ddl = try self.sequenceDdl(.create) };
            if (self.ddlWord("VIEW")) return .{ .catalog_ddl = try self.viewDdl(.create, false) };
            if (self.ddlWord("POLICY")) return .{ .policy_ddl = try self.policyDdl(.create) };
            if (self.keyword(.table)) return .{ .create_table = try self.createTable() };
            const unique = self.keyword(.unique);
            if (self.keyword(.index)) return .{ .catalog_ddl = try self.indexDdl(true, unique) };
            if (unique) return self.fail(error.InvalidSqlSyntax, "UNIQUE requires INDEX");
            return .{ .catalog_ddl = try self.catalogDdl(.create) };
        }
        if (self.keyword(.drop)) {
            if (self.ddlWord("SEQUENCE")) return .{ .catalog_ddl = try self.sequenceDdl(.drop) };
            if (self.ddlWord("VIEW")) return .{ .catalog_ddl = try self.viewDdl(.drop, false) };
            if (self.ddlWord("POLICY")) return .{ .policy_ddl = try self.policyDdl(.drop) };
            if (self.keyword(.index)) return .{ .catalog_ddl = try self.indexDdl(false, false) };
            if (!self.keyword(.table)) return .{ .catalog_ddl = try self.catalogDdl(.drop) };
            const if_exists = self.keyword(.@"if");
            if (if_exists) try self.expectKeyword(.exists);
            return .{ .drop_table = .{ .table = try self.name(), .if_exists = if_exists } };
        }
        if (self.keyword(.alter)) {
            if (self.ddlWord("SEQUENCE")) return .{ .catalog_ddl = try self.sequenceDdl(.alter_sequence) };
            if (self.ddlWord("ROLE")) return .{ .catalog_ddl = try self.roleDefaultDdl() };
            if (self.ddlWord("VIEW")) return .{ .catalog_ddl = try self.viewDdl(.rename, false) };
            if (self.ddlWord("POLICY")) return .{ .policy_ddl = try self.policyDdl(.alter) };
            const before_table = self.pos;
            if (self.keyword(.table)) {
                _ = self.keyword(.only);
                const target = try self.name();
                const change: ?@FieldType(ast.PolicyDdl, "action") = if (self.ddlWord("ENABLE")) .enable else if (self.ddlWord("DISABLE")) .disable else null;
                if (change) |action| {
                    if (!self.ddlWord("ROW") or !self.ddlWord("LEVEL") or !self.ddlWord("SECURITY"))
                        return self.fail(error.InvalidSqlSyntax, "policy publication requires ROW LEVEL SECURITY");
                    return .{ .policy_ddl = .{ .action = action, .name = "", .table = target } };
                }
                self.pos = before_table;
            }
            return .{ .catalog_ddl = try self.catalogDdl(.rename) };
        }
        if (self.keyword(.set)) {
            if (self.keyword(.transaction)) {
                const options = try self.transactionOptions();
                if (!options.isolation_specified and !options.mode_specified and options.deferrable == null) return self.fail(error.InvalidSqlSyntax, "SET TRANSACTION requires a characteristic");
                return .{ .set_transaction = .{
                    .isolation = if (options.isolation_specified) options.isolation else null,
                    .mode = if (options.mode_specified) options.mode else null,
                    .deferrable = options.deferrable,
                } };
            }
            if (!self.ddlWord("CONSTRAINTS")) return self.fail(error.UnsupportedSqlShape, "SET requires CONSTRAINTS");
            var names: std.ArrayList([]const u8) = .empty;
            if (!self.keyword(.all)) while (true) {
                if (names.items.len >= 256) return error.SqlLimitExceeded;
                try names.append(self.alloc, try self.identifier());
                if (!self.take(.comma)) break;
            };
            const deferred = self.ddlWord("DEFERRED");
            if (!deferred and !self.ddlWord("IMMEDIATE")) return self.fail(error.InvalidSqlSyntax, "SET CONSTRAINTS requires DEFERRED or IMMEDIATE");
            return .{ .set_constraints = .{ .names = try names.toOwnedSlice(self.alloc), .deferred = deferred } };
        }
        if (self.keyword(.begin)) {
            _ = self.keyword(.transaction) or self.keyword(.work);
            return .{ .begin = try self.transactionOptions() };
        }
        if (self.keyword(.start)) {
            try self.expectKeyword(.transaction);
            var options = try self.transactionOptions();
            options.syntax = .start_transaction;
            return .{ .begin = options };
        }
        if (self.keyword(.commit)) {
            _ = self.keyword(.transaction) or self.keyword(.work);
            return .commit;
        }
        if (self.keyword(.rollback)) {
            _ = self.keyword(.transaction) or self.keyword(.work);
            if (self.keyword(.to)) {
                _ = self.keyword(.savepoint);
                return .{ .rollback_to_savepoint = try self.identifier() };
            }
            return .rollback;
        }
        if (self.keyword(.savepoint)) return .{ .savepoint = try self.identifier() };
        if (self.keyword(.release)) {
            _ = self.keyword(.savepoint);
            return .{ .release_savepoint = try self.identifier() };
        }
        return self.fail(error.UnsupportedSqlShape, "unsupported SQL statement");
    }

    fn roleDefaultDdl(self: *Parser) Error!ast.CatalogDdl {
        const role = try self.identifier();
        var database: ?[]const u8 = null;
        if (self.keyword(.in)) {
            try self.expectKeyword(.database);
            database = try self.identifier();
        }
        const reset = self.ddlWord("RESET");
        if (!reset) try self.expectKeyword(.set);
        const first = try self.identifier();
        var setting_name = first;
        if (setting_name.len > 128) return self.fail(error.SqlLimitExceeded, "role setting name limit exceeded");
        while (self.take(.dot)) {
            const part = try self.identifier();
            if (part.len + 1 > 128 - setting_name.len) return self.fail(error.SqlLimitExceeded, "role setting name limit exceeded");
            setting_name = try std.fmt.allocPrint(self.alloc, "{s}.{s}", .{ setting_name, part });
        }
        var setting: ?ast.Value = null;
        if (!reset) {
            if (!self.take(.eq)) try self.expectKeyword(.to);
            if (!self.keyword(.default)) {
                if (!self.peek(.string) and !self.peek(.number) and !self.peek(.minus) and !self.peek(.plus) and !self.peek(.placeholder) and !(self.pos < self.tokens.len and (self.tokens[self.pos].isKeyword(.true) or self.tokens[self.pos].isKeyword(.false) or self.tokens[self.pos].isKeyword(.null))))
                    return self.fail(error.InvalidSqlSyntax, "role defaults require a literal value");
                setting = try self.value();
                if (setting.? == .null or setting.? == .parameter) return self.fail(error.InvalidSqlSyntax, "role defaults require a literal value");
            }
        }
        return .{ .kind = .role, .action = .role_default, .name = .{ .table = role }, .role_setting = .{ .name = setting_name, .database = database, .value = setting } };
    }

    fn transactionOptions(self: *Parser) Error!ast.Begin {
        var options: ast.Begin = .{};
        var deferrable = false;
        var isolation_seen = false;
        var mode_seen = false;
        while (true) {
            if (self.keyword(.isolation)) {
                if (isolation_seen) return self.fail(error.InvalidSqlSyntax, "duplicate transaction isolation mode");
                isolation_seen = true;
                options.isolation_specified = true;
                try self.expectKeyword(.level);
                if (self.keyword(.serializable)) options.isolation = .serializable else if (self.keyword(.repeatable)) {
                    try self.expectKeyword(.read);
                    options.isolation = .repeatable_read;
                } else {
                    try self.expectKeyword(.read);
                    try self.expectKeyword(.committed);
                    options.isolation = .read_committed;
                }
            } else if (self.keyword(.read)) {
                if (mode_seen) return self.fail(error.InvalidSqlSyntax, "duplicate transaction access mode");
                mode_seen = true;
                options.mode_specified = true;
                if (self.keyword(.only)) options.mode = .read_only else {
                    try self.expectKeyword(.write);
                    options.mode = .read_write;
                }
            } else if (self.keyword(.deferrable)) {
                deferrable = true;
                options.deferrable = true;
            } else if (self.pos + 1 < self.tokens.len and self.tokens[self.pos].isKeyword(.not) and self.tokens[self.pos + 1].isKeyword(.deferrable)) {
                self.pos += 2;
                deferrable = false;
                options.deferrable = false;
            } else break;
            if (self.take(.comma) and (self.pos >= self.tokens.len or self.tokens[self.pos].kind == .semicolon)) return self.fail(error.InvalidSqlSyntax, "transaction mode required after comma");
        }
        // PostgreSQL DEFERRABLE has no effect except for SERIALIZABLE READ
        // ONLY. That stronger safe-snapshot promise needs native admission;
        // never silently label an ordinary retained snapshot as deferrable.
        if (deferrable and options.isolation == .serializable and options.mode == .read_only) return self.fail(error.UnsupportedSqlShape, "serializable read-only DEFERRABLE requires safe snapshot admission");
        return options;
    }
};

test "compiler SQL COMMENT retains qualified table identity literal text and NULL removal" {
    // sql-0142: presentation is not an ALTER COLUMN or schema rewrite.
    for ([_]struct { sql: []const u8, text: ?[]const u8 }{
        .{ .sql = "COMMENT ON TABLE usage_records IS 'metered usage rows';", .text = "metered usage rows" },
        .{ .sql = "COMMENT ON TABLE appdb.public.usage_records IS 'a ''quoted'' Ω';", .text = "a 'quoted' Ω" },
        .{ .sql = "COMMENT ON TABLE usage_records IS NULL", .text = null },
        .{ .sql = "COMMENT ON TABLE usage_records IS ''", .text = "" },
    }, 0..) |case, index| {
        var compiled = try compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        const ddl = compiled.statement.catalog_ddl;
        try std.testing.expectEqual(.table, ddl.kind);
        try std.testing.expectEqual(.comment, ddl.action);
        try std.testing.expectEqualStrings("usage_records", ddl.name.table);
        if (index == 1) {
            try std.testing.expectEqualStrings("appdb", ddl.name.database.?);
            try std.testing.expectEqualStrings("public", ddl.name.namespace.?);
        }
        if (case.text) |text| try std.testing.expectEqualStrings(text, ddl.comment_value.?) else try std.testing.expect(ddl.comment_value == null);
    }
    for ([_][]const u8{
        "COMMENT ON TABLE usage_records IS 42",
        "COMMENT ON TABLE usage_records IS $1",
        "COMMENT ON TABLE usage_records IS lower('x')",
        "COMMENT ON TABLE usage_records 'x'",
    }) |sql| try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, sql, .{}));
    try std.testing.expectError(error.UnsupportedSqlShape, compile(std.testing.allocator, "COMMENT ON TABLE usage_records IS 'x'; SELECT 1", .{}));
}

test "compiler role defaults retain exact role database literal and RESET intent" {
    for ([_][]const u8{
        "ALTER ROLE app_writer SET app.tenant_id = 'acme'",
        "ALTER ROLE app_writer IN DATABASE appdb SET app.tenant_id = 'acme'",
        "ALTER ROLE app_writer RESET app.tenant_id",
        "ALTER ROLE app_writer SET statement_timeout = '1ms'",
        "ALTER ROLE app_writer IN DATABASE appdb SET statement_timeout = '1ms'",
        "ALTER ROLE app_writer RESET statement_timeout",
    }, 0..) |sql, index| {
        var compiled = try compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        const ddl = compiled.statement.catalog_ddl;
        try std.testing.expectEqual(.role, ddl.kind);
        try std.testing.expectEqual(.role_default, ddl.action);
        try std.testing.expectEqualStrings("app_writer", ddl.name.table);
        const setting = ddl.role_setting.?;
        try std.testing.expectEqualStrings(if (index < 3) "app.tenant_id" else "statement_timeout", setting.name);
        try std.testing.expectEqual(index == 1 or index == 4, setting.database != null);
        if (setting.database) |database| try std.testing.expectEqualStrings("appdb", database);
        try std.testing.expectEqual(index != 2 and index != 5, setting.value != null);
    }
    try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, "ALTER ROLE app_writer SET app.tenant_id = current_setting('app.tenant_id')", .{}));
    try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, "ALTER ROLE app_writer SET app.tenant_id = $1", .{}));
    try std.testing.expectError(error.UnsupportedSqlShape, compile(std.testing.allocator, "ALTER ROLE app_writer RESET app.tenant_id; SELECT 1", .{}));
}

test "compiler SET TRANSACTION preserves omitted characteristics" {
    var readonly = try compile(std.testing.allocator, "SET TRANSACTION READ ONLY", .{});
    defer readonly.deinit();
    try std.testing.expect(readonly.statement.set_transaction.isolation == null);
    try std.testing.expectEqual(@import("session.zig").ReadMode.read_only, readonly.statement.set_transaction.mode.?);
    var original = try compile(std.testing.allocator, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY, NOT DEFERRABLE", .{});
    defer original.deinit();
    try std.testing.expectEqual(@import("session.zig").Isolation.repeatable_read, original.statement.set_transaction.isolation.?);
    try std.testing.expectEqual(false, original.statement.set_transaction.deferrable.?);
    try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, "SET TRANSACTION", .{}));
    try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, "SET TRANSACTION READ ONLY READ WRITE", .{}));
}

test "compiler transaction DEFERRABLE only claims implemented snapshot semantics" {
    for ([_][]const u8{ "BEGIN READ WRITE DEFERRABLE", "BEGIN DEFERRABLE READ WRITE", "BEGIN NOT DEFERRABLE" }) |sql| {
        var parsed = try compile(std.testing.allocator, sql, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(@import("session.zig").ReadMode.read_write, parsed.statement.begin.mode);
        try std.testing.expectEqual(parsed.statement.begin.deferrable orelse false, parsed.statement.begin.options().deferrable);
    }
    var ignored = try compile(std.testing.allocator, "BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY DEFERRABLE", .{});
    defer ignored.deinit();
    try std.testing.expectEqual(@import("session.zig").Isolation.read_committed, ignored.statement.begin.isolation);
    try std.testing.expectEqual(@import("session.zig").ReadMode.read_only, ignored.statement.begin.mode);
    try std.testing.expectError(error.UnsupportedSqlShape, compile(std.testing.allocator, "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE", .{}));
}

test "compiler deferred uniqueness and constraint timing are explicit" {
    var declaration = try compile(std.testing.allocator, "ALTER TABLE rows ADD CONSTRAINT u UNIQUE (id) DEFERRABLE INITIALLY DEFERRED", .{});
    defer declaration.deinit();
    try std.testing.expect(declaration.statement.catalog_ddl.schema_change.?.add_unique.deferrable);
    try std.testing.expectEqualStrings("deferred", declaration.statement.catalog_ddl.schema_change.?.add_unique.timing);
    var immediate = try compile(std.testing.allocator, "ALTER TABLE rows ADD CONSTRAINT u UNIQUE (id) NOT DEFERRABLE", .{});
    defer immediate.deinit();
    try std.testing.expect(!immediate.statement.catalog_ddl.schema_change.?.add_unique.deferrable);
    try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, "ALTER TABLE rows ADD CONSTRAINT u UNIQUE (id) INITIALLY DEFERRED", .{}));
    var all = try compile(std.testing.allocator, "SET CONSTRAINTS ALL DEFERRED", .{});
    defer all.deinit();
    try std.testing.expect(all.statement.set_constraints.deferred);
    try std.testing.expectEqual(@as(usize, 0), all.statement.set_constraints.names.len);
    var named = try compile(std.testing.allocator, "SET CONSTRAINTS u, \"Other\" IMMEDIATE", .{});
    defer named.deinit();
    try std.testing.expect(!named.statement.set_constraints.deferred);
    try std.testing.expectEqualStrings("Other", named.statement.set_constraints.names[1]);
}

test "constraint name provenance distinguishes explicit identifiers from generated placeholders" {
    const cases = [_]struct { sql: []const u8, generated: []const bool }{
        .{ .sql = "CREATE TABLE t (id BIGINT PRIMARY KEY, email TEXT UNIQUE, CHECK (id > 0), FOREIGN KEY (id) REFERENCES parent (id))", .generated = &.{ true, true, true, true } },
        .{ .sql = "CREATE TABLE t (id BIGINT CONSTRAINT sql_primary_0 PRIMARY KEY, email TEXT CONSTRAINT sql_unique_1 UNIQUE, CONSTRAINT sql_constraint_2 CHECK (id > 0), CONSTRAINT sql_inline_3 FOREIGN KEY (id) REFERENCES parent (id))", .generated = &.{ false, false, false, false } },
        .{ .sql = "CREATE TABLE t (id BIGINT CHECK (id > 0) REFERENCES parent (id), email TEXT, UNIQUE (email), PRIMARY KEY (id))", .generated = &.{ true, true, true, true } },
    };
    for (cases) |case| {
        var compiled = try compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        const constraints = compiled.statement.create_table.constraints;
        try std.testing.expectEqual(case.generated.len, constraints.len);
        for (constraints, case.generated) |constraint, generated| try std.testing.expectEqual(generated, constraint.generatedConstraintName() != null);
    }
    var altered = try compile(std.testing.allocator, "ALTER TABLE t ADD CONSTRAINT sql_unique_0 UNIQUE (email)", .{});
    defer altered.deinit();
    try std.testing.expect(altered.statement.catalog_ddl.schema_change.?.generatedConstraintName() == null);
}

test "policy DDL parses draft definitions and never accepts publication commands" {
    var created = try compile(std.testing.allocator, "CREATE POLICY tenant_filter ON public.accounts AS RESTRICTIVE FOR UPDATE TO tenant_admin USING (tenant_id = 7) WITH CHECK (tenant_id = 7)", .{});
    defer created.deinit();
    const policy = created.statement.policy_ddl;
    try std.testing.expectEqual(.create, policy.action);
    try std.testing.expectEqualStrings("tenant_filter", policy.name);
    try std.testing.expectEqualStrings("accounts", policy.table.table);
    try std.testing.expectEqual(.update, policy.command);
    try std.testing.expect(!policy.permissive);
    try std.testing.expectEqualStrings("tenant_admin", policy.roles[0]);
    try std.testing.expect(policy.using != null and policy.with_check != null);

    var altered = try compile(std.testing.allocator, "ALTER POLICY tenant_filter ON public.accounts TO tenant_admin, auditor USING (tenant_id = 8)", .{});
    defer altered.deinit();
    try std.testing.expectEqual(.alter, altered.statement.policy_ddl.action);
    try std.testing.expect(altered.statement.policy_ddl.using_specified);
    try std.testing.expect(!altered.statement.policy_ddl.check_specified);
    try std.testing.expectEqual(@as(usize, 2), altered.statement.policy_ddl.roles.len);

    var dropped = try compile(std.testing.allocator, "DROP POLICY IF EXISTS tenant_filter ON public.accounts", .{});
    defer dropped.deinit();
    try std.testing.expect(dropped.statement.policy_ddl.if_exists);
    var enabled = try compile(std.testing.allocator, "ALTER TABLE public.accounts ENABLE ROW LEVEL SECURITY", .{});
    defer enabled.deinit();
    try std.testing.expectEqual(.enable, enabled.statement.policy_ddl.action);
    var disabled = try compile(std.testing.allocator, "ALTER TABLE public.accounts DISABLE ROW LEVEL SECURITY", .{});
    defer disabled.deinit();
    try std.testing.expectEqual(.disable, disabled.statement.policy_ddl.action);
    try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, "CREATE POLICY bad ON accounts FOR INSERT USING (true)", .{}));
    try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, "CREATE POLICY bad ON accounts FOR SELECT WITH CHECK (true)", .{}));
}

test "compiler UNIQUE null semantics retain PostgreSQL clause ordering" {
    const alloc = std.testing.allocator;
    for ([_]struct { sql: []const u8, not_distinct: bool }{
        .{ .sql = "CREATE UNIQUE INDEX email_key ON items (email) NULLS NOT DISTINCT", .not_distinct = true },
        .{ .sql = "CREATE UNIQUE INDEX email_key ON items (email) NULLS DISTINCT", .not_distinct = false },
        .{ .sql = "CREATE UNIQUE INDEX email_key ON items (lower(email)) INCLUDE (id) NULLS NOT DISTINCT WHERE email IS NULL", .not_distinct = true },
        .{ .sql = "CREATE INDEX email_lookup ON items (email) NULLS NOT DISTINCT", .not_distinct = true },
    }) |case| {
        var compiled = try compile(alloc, case.sql, .{});
        defer compiled.deinit();
        try std.testing.expectEqual(case.not_distinct, compiled.statement.catalog_ddl.schema_change.?.create_index.nulls_not_distinct);
    }
    for ([_]struct { sql: []const u8, not_distinct: bool }{
        .{ .sql = "CREATE TABLE items (email TEXT UNIQUE NULLS NOT DISTINCT)", .not_distinct = true },
        .{ .sql = "CREATE TABLE items (email TEXT CONSTRAINT email_key UNIQUE NULLS NOT DISTINCT DEFERRABLE INITIALLY DEFERRED)", .not_distinct = true },
        .{ .sql = "CREATE TABLE items (email TEXT, UNIQUE NULLS DISTINCT (email))", .not_distinct = false },
        .{ .sql = "CREATE TABLE items (email TEXT, CONSTRAINT email_key UNIQUE NULLS NOT DISTINCT (email))", .not_distinct = true },
    }) |case| {
        var compiled = try compile(alloc, case.sql, .{});
        defer compiled.deinit();
        try std.testing.expectEqual(case.not_distinct, compiled.statement.create_table.constraints[0].add_unique.nulls_not_distinct);
    }
    var altered = try compile(alloc, "ALTER TABLE items ADD CONSTRAINT email_key UNIQUE NULLS NOT DISTINCT (email) DEFERRABLE INITIALLY DEFERRED", .{});
    defer altered.deinit();
    const constraint = altered.statement.catalog_ddl.schema_change.?.add_unique;
    try std.testing.expect(constraint.nulls_not_distinct and constraint.deferrable);
    try std.testing.expectEqualStrings("deferred", constraint.timing);
    for ([_][]const u8{
        "CREATE UNIQUE INDEX bad ON items (email) NULLS NOT",
        "CREATE UNIQUE INDEX bad ON items (email) NULLS FIRST",
        "CREATE UNIQUE INDEX bad ON items (email) NULLS DISTINCT NULLS DISTINCT",
        "CREATE UNIQUE INDEX bad ON items (email) NULLS NOT DISTINCT INCLUDE (id)",
        "CREATE TABLE items (email TEXT PRIMARY KEY NULLS NOT DISTINCT)",
        "ALTER TABLE items ADD CONSTRAINT bad UNIQUE (email) NULLS NOT DISTINCT",
    }) |sql| try std.testing.expectError(error.InvalidSqlSyntax, compile(alloc, sql, .{}));
}

test "compiler original uniqueness null clauses retain unchanged source contracts" {
    const alloc = std.testing.allocator;
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, @embedFile("fixtures/sql_parity_inventory.json"), .{});
    defer parsed.deinit();
    var covered: usize = 0;
    for (parsed.value.object.get("entries").?.array.items) |entry| {
        const id = entry.object.get("id").?.string;
        const not_distinct = std.mem.eql(u8, id, "sql-0692");
        const constraint = std.mem.eql(u8, id, "sql-0731");
        if (!not_distinct and !constraint and !std.mem.eql(u8, id, "sql-0706")) continue;
        var compiled = try compile(alloc, entry.object.get("sql").?.string, .{});
        defer compiled.deinit();
        const ddl = compiled.statement.catalog_ddl;
        if (constraint) {
            try std.testing.expect(!ddl.schema_change.?.add_unique.nulls_not_distinct);
            try std.testing.expect(!ddl.schema_change.?.add_unique.deferrable);
            try std.testing.expectEqualStrings("immediate", ddl.schema_change.?.add_unique.timing);
            try std.testing.expectEqualStrings("usage_records", ddl.name.table);
        } else {
            try std.testing.expectEqual(not_distinct, ddl.schema_change.?.create_index.nulls_not_distinct);
            try std.testing.expect(ddl.schema_change.?.create_index.unique);
            try std.testing.expectEqualStrings("usage_records", ddl.index_table.?.table);
        }
        covered += 1;
    }
    try std.testing.expectEqual(@as(usize, 3), covered);
    // Compiler evidence is not mounted activation or original-case credit.
}

test "compiler index DDL retains namespace ownership and distributed statement contracts" {
    var create = try compile(std.testing.allocator, "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS email_key ON ONLY tenant.accounts (lower(email)) INCLUDE (id) WHERE email IS NOT NULL", .{});
    defer create.deinit();
    const index = create.statement.catalog_ddl;
    try std.testing.expectEqual(.index, index.kind);
    try std.testing.expectEqual(.create, index.action);
    try std.testing.expect(index.concurrently and index.conditional);
    try std.testing.expectEqualStrings("email_key", index.name.table);
    try std.testing.expectEqualStrings("tenant", index.index_table.?.namespace.?);
    try std.testing.expectEqualStrings("accounts", index.index_table.?.table);
    try std.testing.expect(index.schema_change.?.create_index.unique);
    var drop = try compile(std.testing.allocator, "DROP INDEX IF EXISTS tenant.email_key, public.other_key CASCADE", .{});
    defer drop.deinit();
    const targets = drop.statement.catalog_ddl;
    try std.testing.expectEqual(.index, targets.kind);
    try std.testing.expectEqual(.drop, targets.action);
    try std.testing.expectEqual(@as(usize, 2), targets.index_targets.len);
    try std.testing.expect(targets.cascade and targets.conditional);
    try std.testing.expectEqualStrings("tenant", targets.index_targets[0].namespace.?);
    try std.testing.expectEqualStrings("other_key", targets.index_targets[1].table);
    var concurrent = try compile(std.testing.allocator, "DROP INDEX CONCURRENTLY tenant.email_key RESTRICT", .{});
    defer concurrent.deinit();
    try std.testing.expect(concurrent.statement.catalog_ddl.concurrently);
    try std.testing.expect(!concurrent.statement.catalog_ddl.cascade);
    // CREATE names inherit the table's schema; DROP is not table-bound syntax.
    try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, "CREATE INDEX tenant.email_key ON tenant.accounts (email)", .{}));
    var scoped = try compile(std.testing.allocator, "DROP INDEX email_key ON accounts", .{});
    defer scoped.deinit();
    try std.testing.expectEqualStrings("accounts", scoped.statement.catalog_ddl.name.table);
}

test "compiler ONLY scopes exact table references across read and write statements" {
    for ([_][]const u8{
        "SELECT id FROM ONLY public.records",
        "INSERT INTO ONLY public.records (id) VALUES ('a') RETURNING id",
        "UPDATE ONLY public.records SET id='a' FROM ONLY public.refs AS s WHERE records.id=s.id",
        "DELETE FROM ONLY public.records USING ONLY public.refs AS s WHERE records.id=s.id",
        "MERGE INTO ONLY public.records AS r USING ONLY public.refs AS s ON r.id=s.id WHEN MATCHED THEN DELETE",
        "TRUNCATE ONLY public.records",
        "ALTER TABLE ONLY public.records VALIDATE CONSTRAINT c",
        "CREATE INDEX idx ON ONLY public.records (id)",
    }) |sql| {
        var compiled = try compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
    }
    var inserted = try compile(std.testing.allocator, "INSERT INTO ONLY public.records (id) VALUES ('a')", .{});
    defer inserted.deinit();
    try std.testing.expectEqualStrings("public", inserted.statement.insert.table.namespace.?);
    try std.testing.expectEqualStrings("records", inserted.statement.insert.table.table);
    var quoted = try compile(std.testing.allocator, "SELECT id FROM \"only\"", .{});
    defer quoted.deinit();
    try std.testing.expectEqualStrings("only", quoted.statement.select.table.?.table);
}

test "compiler UPDATE row assignments flatten simultaneous typed expressions" {
    var compiled = try compile(std.testing.allocator, "UPDATE rows SET (quantity, status) = ROW((SELECT quantity FROM rows WHERE id = 'source'), 'copied') WHERE id = 'target' RETURNING id", .{});
    defer compiled.deinit();
    const assignments = compiled.statement.update.assignments;
    try std.testing.expectEqual(@as(usize, 2), assignments.len);
    try std.testing.expectEqualStrings("quantity", assignments[0].field);
    try std.testing.expect(assignments[0].expression != null);
    try std.testing.expectEqualStrings("status", assignments[1].field);
    try std.testing.expectEqualStrings("copied", assignments[1].value.string);
    var bare = try compile(std.testing.allocator, "UPDATE rows SET (quantity, status) = (7, 'ready')", .{});
    defer bare.deinit();
    try std.testing.expectEqual(@as(i64, 7), bare.statement.update.assignments[0].value.integer);
    var defaults = try compile(std.testing.allocator, "UPDATE rows SET (quantity, status) = ROW(DEFAULT, 'ready'), other=DEFAULT", .{});
    defer defaults.deinit();
    try std.testing.expect(defaults.statement.update.assignments[0].use_default);
    try std.testing.expect(!defaults.statement.update.assignments[1].use_default);
    try std.testing.expect(defaults.statement.update.assignments[2].use_default);
    try std.testing.expectError(error.DuplicateSqlAssignment, compile(std.testing.allocator, "UPDATE rows SET (quantity, quantity) = ROW(1, 2)", .{}));
    try std.testing.expectError(error.DuplicateSqlAssignment, compile(std.testing.allocator, "UPDATE rows SET (quantity, status) = ROW(1, 'x'), status='y'", .{}));
    try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, "UPDATE rows SET (quantity, status) = ROW(1)", .{}));
    try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, "UPDATE rows SET (quantity, status) = ROW(1, 'x', 3)", .{}));
    var queried = try compile(std.testing.allocator, "UPDATE rows SET (quantity, status) = (SELECT n, 'copied' FROM source ORDER BY n LIMIT 1)", .{});
    defer queried.deinit();
    try std.testing.expectEqual(@as(usize, 2), queried.statement.update.assignments.len);
    for (queried.statement.update.assignments) |assignment| {
        try std.testing.expect(assignment.expression.?.* == .call);
        try std.testing.expectEqualStrings("$row_scalar", assignment.expression.?.call.name);
    }
    for ([_][]const u8{
        "UPDATE rows SET (quantity,status)=(SELECT * FROM source)",
        "UPDATE rows SET (quantity,status)=(SELECT source.* FROM source)",
        "UPDATE rows SET (quantity,status)=(SELECT source.*,'copied' FROM source)",
        "UPDATE rows SET (quantity)=(SELECT COUNT(*) FROM source)",
    }) |sql| {
        var deferred = try compile(std.testing.allocator, sql, .{});
        defer deferred.deinit();
        const mutation = deferred.statement.update;
        try std.testing.expectEqual(@as(?usize, mutation.assignments.len), mutation.assignments[0].expression.?.call.subquery.?.required_output_columns);
    }
    try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, "UPDATE rows SET (quantity, status) = (SELECT n FROM source)", .{}));
    try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, "UPDATE rows SET (quantity, status) = (SELECT n, status, id FROM source)", .{}));
    try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, "WITH \"\x00$update_row_source_0\" AS (SELECT n FROM source) UPDATE rows SET (quantity, status) = (SELECT n, 'copied' FROM source)", .{}));
}

test "compiler INSERT defaults retain per-row omission beside scalar sources" {
    var values = try compile(std.testing.allocator, "INSERT INTO rows (id,status,n) VALUES ('a',DEFAULT,1),('b','set',DEFAULT)", .{});
    defer values.deinit();
    try std.testing.expectEqual(@as(usize, 2), values.statement.insert.rows.len);
    try std.testing.expect(values.statement.insert.isDefault(0, 1));
    try std.testing.expect(values.statement.insert.isDefault(1, 2));
    try std.testing.expect(!values.statement.insert.isDefault(0, 2));
    var sourced = try compile(std.testing.allocator, "INSERT INTO rows (id,status,n) VALUES ('a',DEFAULT,(SELECT 1)),('b',(SELECT 'set'),DEFAULT)", .{});
    defer sourced.deinit();
    try std.testing.expect(sourced.statement.insert.source != null);
    try std.testing.expect(sourced.statement.insert.isDefault(0, 1));
    try std.testing.expect(sourced.statement.insert.isDefault(1, 2));
    var all_default = try compile(std.testing.allocator, "INSERT INTO rows DEFAULT VALUES", .{});
    defer all_default.deinit();
    try std.testing.expectEqual(@as(usize, 1), all_default.statement.insert.rows.len);
    try std.testing.expectEqual(@as(usize, 0), all_default.statement.insert.columns.len);
}

test "compiler conditional schema ALTER preserves atomic absence admission" {
    var added = try compile(std.testing.allocator, "ALTER TABLE IF EXISTS missing_records ADD COLUMN optional_status text", .{});
    defer added.deinit();
    try std.testing.expect(added.statement.catalog_ddl.conditional);
    try std.testing.expectEqual(.alter_schema, added.statement.catalog_ddl.action);
    var dropped = try compile(std.testing.allocator, "ALTER TABLE IF EXISTS missing_records DROP COLUMN optional_status", .{});
    defer dropped.deinit();
    try std.testing.expect(dropped.statement.catalog_ddl.conditional);
    try std.testing.expectEqual(.alter_schema, dropped.statement.catalog_ddl.action);
    try std.testing.expectError(error.UnsupportedSqlShape, compile(std.testing.allocator, "ALTER TABLE IF EXISTS missing_records RENAME TO renamed_records", .{}));
    try std.testing.expectError(error.UnsupportedSqlShape, compile(std.testing.allocator, "ALTER TABLE IF EXISTS missing_records SET TABLESPACE fast", .{}));
}

test "compiler reserved named argument diagnostics do not reinterpret quoted identifiers or strings" {
    try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, "SELECT unsupported_fn(limit => 1)", .{}));
    try std.testing.expectError(error.UnsupportedSqlShape, compile(std.testing.allocator, "SELECT unsupported_fn(\"limit\" => 1)", .{}));
    var text = try compile(std.testing.allocator, "SELECT 'limit => 1'", .{});
    defer text.deinit();
}

test "compiler transaction modes and quoted savepoints are explicit" {
    var begin = try compile(std.testing.allocator, "START TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY", .{});
    defer begin.deinit();
    try std.testing.expectEqual(@import("session.zig").Isolation.repeatable_read, begin.statement.begin.isolation);
    try std.testing.expectEqual(@import("session.zig").ReadMode.read_only, begin.statement.begin.mode);
    try std.testing.expectEqualStrings("START TRANSACTION", begin.statement.begin.commandTag());
    var ordinary = try compile(std.testing.allocator, "BEGIN WORK", .{});
    defer ordinary.deinit();
    try std.testing.expectEqualStrings("BEGIN", ordinary.statement.begin.commandTag());
    try std.testing.expectEqual(@import("session.zig").Isolation.repeatable_read, begin.statement.begin.options().isolation);
    var rollback = try compile(std.testing.allocator, "ROLLBACK TO SAVEPOINT \"Before Update\"", .{});
    defer rollback.deinit();
    try std.testing.expectEqualStrings("Before Update", rollback.statement.rollback_to_savepoint);
    var release = try compile(std.testing.allocator, "RELEASE before_update", .{});
    defer release.deinit();
    try std.testing.expectEqualStrings("before_update", release.statement.release_savepoint);
    try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, "BEGIN READ ONLY READ WRITE", .{}));
}

test "compiler owns folded and quoted identifiers and exact literals" {
    const source = try std.testing.allocator.dupe(u8, "INSERT INTO db.ns.Accounts (_id, \"UserName\", balance) VALUES ('1', 'Ada', -9223372036854775808), ('2', $1, 9223372036854775807);");
    var compiled = try compile(std.testing.allocator, source, .{});
    std.testing.allocator.free(source);
    defer compiled.deinit();
    const insert = compiled.statement.insert;
    try std.testing.expectEqualStrings("accounts", insert.table.table);
    try std.testing.expectEqualStrings("UserName", insert.columns[1]);
    try std.testing.expectEqualStrings("Ada", insert.rows[0][1].string);
    try std.testing.expectEqual(std.math.minInt(i64), insert.rows[0][2].integer);
    try std.testing.expectEqual(std.math.maxInt(i64), insert.rows[1][2].integer);
    try std.testing.expectEqual(@as(u32, 1), compiled.parameter_count);
}

test "compiler conflict row assignments preserve defaults simultaneous expressions and bounded ownership" {
    const Fixture = struct {
        fn run(a: std.mem.Allocator) !void {
            var compiled = try compile(a, "INSERT INTO users (id,age) VALUES ('a',2) ON CONFLICT (id) DO UPDATE SET (name,age)=ROW(DEFAULT,users.age+excluded.age),active=true RETURNING age", .{});
            defer compiled.deinit();
            const assignments = compiled.statement.insert.conflict.?.assignments;
            try std.testing.expectEqual(@as(usize, 3), assignments.len);
            try std.testing.expectEqualStrings("name", assignments[0].field);
            try std.testing.expect(assignments[0].use_default);
            try std.testing.expectEqualStrings("age", assignments[1].field);
            try std.testing.expectEqual(ast.Scalar.Binary.add, assignments[1].expression.?.binary.op);
            try std.testing.expectEqualStrings("active", assignments[2].field);
            try std.testing.expect(assignments[2].expression.?.literal.boolean);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
    for ([_][]const u8{
        "INSERT INTO users(id) VALUES('a') ON CONFLICT(id) DO UPDATE SET (age,age)=ROW(1,2)",
        "INSERT INTO users(id) VALUES('a') ON CONFLICT(id) DO UPDATE SET (name,age)=ROW('x',2),age=3",
    }) |sql| try std.testing.expectError(error.DuplicateSqlAssignment, compile(std.testing.allocator, sql, .{}));
    for ([_][]const u8{
        "INSERT INTO users(id) VALUES('a') ON CONFLICT(id) DO UPDATE SET (name,age)=ROW('x')",
        "INSERT INTO users(id) VALUES('a') ON CONFLICT(id) DO UPDATE SET (name,age)=ROW('x',2,3)",
    }) |sql| try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, sql, .{}));
}

test "compiler keeps ON CONFLICT arbiter predicate separate from update predicate" {
    var compiled = try compile(std.testing.allocator, "INSERT INTO users (email, active) VALUES ('a', true) ON CONFLICT (email) WHERE active = true DO UPDATE SET active = false WHERE users.active = true", .{});
    defer compiled.deinit();
    const conflict = compiled.statement.insert.conflict.?;
    try std.testing.expect(conflict.arbiter_predicate != null);
    try std.testing.expect(conflict.predicate != null);
    try std.testing.expect(conflict.arbiter_predicate.?.* == .binary);
    try std.testing.expectEqual(ast.Scalar.Binary.eq, conflict.arbiter_predicate.?.binary.op);
    try std.testing.expectEqual(ast.Scalar.Binary.eq, conflict.predicate.?.binary.op);
}

test "compiler keeps typed parameters and boolean precedence" {
    var compiled = try compile(std.testing.allocator, "SELECT _id, name AS display FROM public.users WHERE active = true OR age >= $1 AND NOT name IS NULL ORDER BY age DESC LIMIT $2 OFFSET 4", .{});
    defer compiled.deinit();
    const select = compiled.statement.select;
    try std.testing.expectEqualStrings("public", select.table.?.namespace.?);
    try std.testing.expectEqualStrings("display", select.columns[1].alias.?);
    try std.testing.expectEqual(@as(u32, 1), select.predicate.?.disjunction.right.conjunction.left.comparison.value.parameter);
    try std.testing.expect(select.predicate.?.disjunction.right.conjunction.right.* == .negation);
    try std.testing.expect(select.order_by[0].descending);
    try std.testing.expectEqual(@as(u32, 2), select.limit.?.parameter);
}

test "compiler pagination normalizes bounds and rejects duplicate or incomplete clauses" {
    var compiled = try compile(std.testing.allocator, "SELECT 1 OFFSET $1 ROWS FETCH NEXT $2 ROWS ONLY", .{});
    defer compiled.deinit();
    try std.testing.expectEqual(@as(u32, 1), compiled.statement.select.offset.?.parameter);
    try std.testing.expectEqual(@as(u32, 2), compiled.statement.select.limit.?.parameter);
    for ([_][]const u8{
        "SELECT 1 FETCH",
        "SELECT 1 FETCH NEXT",
        "SELECT 1 FETCH NEXT ROW",
        "SELECT 1 LIMIT 1 LIMIT 2",
        "SELECT 1 OFFSET 1 OFFSET 2",
        "SELECT 1 LIMIT ALL FETCH FIRST ROW ONLY",
        "SELECT 1 FETCH FIRST ROW ONLY LIMIT 1",
    }) |sql| try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, sql, .{}));
    try std.testing.expectError(error.UnsupportedSqlShape, compile(std.testing.allocator, "SELECT 1 ORDER BY 1 FETCH FIRST ROW WITH TIES", .{}));
}

test "compiler preserves declarative composite and natural join keys" {
    var compiled = try compile(std.testing.allocator, "SELECT * FROM t FULL OUTER JOIN u USING (b, a) AS merged NATURAL LEFT JOIN v", .{});
    defer compiled.deinit();
    const outer = compiled.statement.select.source.?.join;
    try std.testing.expectEqual(ast.JoinKind.left, outer.kind);
    try std.testing.expect(outer.keys == .natural);
    const inner = outer.left.join;
    try std.testing.expectEqual(ast.JoinKind.full, inner.kind);
    try std.testing.expectEqualStrings("b", inner.keys.using[0]);
    try std.testing.expectEqualStrings("a", inner.keys.using[1]);
    try std.testing.expectEqualStrings("merged", inner.using_alias.?);
    try std.testing.expect(inner.condition == null);
    for ([_][]const u8{
        "SELECT * FROM t JOIN u USING ()",
        "SELECT * FROM t JOIN u USING (a,)",
        "SELECT * FROM t NATURAL CROSS JOIN u",
        "SELECT * FROM t NATURAL JOIN u ON true",
        "SELECT * FROM t NATURAL JOIN u USING (a)",
    }) |sql| {
        if (compile(std.testing.allocator, sql, .{})) |value_| {
            var value = value_;
            value.deinit();
            return error.TestUnexpectedResult;
        } else |_| {}
    }
}

test "compiler rejects unsupported clauses and additional statements atomically" {
    const cases = [_][]const u8{
        "SELECT * FROM t; DELETE FROM t",
        "INSERT INTO t (x) VALUES (1) ON CONFLICT DO UPDATE SET x=2",
        "DELETE FROM t RETURNING *; INSERT INTO t (x) VALUES (1)",
    };
    for (cases) |source| {
        const result = compile(std.testing.allocator, source, .{});
        if (result) |value_| {
            var value = value_;
            value.deinit();
            return error.TestUnexpectedResult;
        } else |_| {}
    }
}

test "compiler bounds resources and validates parameters and numbers" {
    try std.testing.expectError(error.SqlLimitExceeded, compile(std.testing.allocator, "SELECT * FROM t", .{ .max_bytes = 3 }));
    try std.testing.expectError(error.SqlLimitExceeded, compile(std.testing.allocator, "SELECT * FROM t", .{ .max_tokens = 2 }));
    try std.testing.expectError(error.SqlLimitExceeded, compile(std.testing.allocator, "SELECT a,b FROM t", .{ .max_nodes = 1 }));
    try std.testing.expectError(error.SqlLimitExceeded, compile(std.testing.allocator, "SELECT * FROM t WHERE (((a=1)))", .{ .max_depth = 2 }));
    try std.testing.expectError(error.InvalidSqlParameter, compile(std.testing.allocator, "DELETE FROM t WHERE x=$0", .{}));
    try std.testing.expectError(error.InvalidSqlParameter, compile(std.testing.allocator, "DELETE FROM t WHERE x=$1025", .{}));
    for ([_][]const u8{ "SELECT 9223372036854775808", "SELECT 1e9999" }) |sql| {
        var exact = try compile(std.testing.allocator, sql, .{});
        defer exact.deinit();
        try std.testing.expect(exact.statement.select.columns[0].expression.?.literal == .numeric);
    }
    try std.testing.expectError(error.DuplicateSqlAssignment, compile(std.testing.allocator, "UPDATE t SET x=1,X=2", .{}));
}

test "compiler DDL literal defaults and count" {
    var ddl = try compile(std.testing.allocator, "CREATE TABLE IF NOT EXISTS t (name text NOT NULL, age bigint DEFAULT 0, enabled boolean DEFAULT true)", .{});
    defer ddl.deinit();
    try std.testing.expect(ddl.statement.create_table.if_not_exists);
    try std.testing.expect(!ddl.statement.create_table.columns[0].nullable);
    try std.testing.expectEqual(@as(i64, 0), ddl.statement.create_table.columns[1].default_expression.?.literal.integer);
    var count = try compile(std.testing.allocator, "SELECT count(*) AS total FROM t", .{});
    defer count.deinit();
    try std.testing.expect(count.statement.select.count_all);
    try std.testing.expectEqualStrings("total", count.statement.select.count_alias.?);
}

test "compiler predicate modifiers preserve keyword quoting and unwind allocation faults" {
    for ([_][]const u8{ "TRUE IS \"unknown\"", "7 BETWEEN \"symmetric\" 9 AND 2", "7 BETWEEN SYMMETRIC ASYMMETRIC 9 AND 2" }) |sql| {
        if (compileScalar(std.testing.allocator, sql, .{})) |result| {
            var owned = result;
            owned.deinit();
            return error.ExpectedPredicateSyntaxFailure;
        } else |_| {}
    }
    const Fixture = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var compiled = try compileScalar(alloc, "($1 BETWEEN SYMMETRIC $2 AND $3) IS NOT UNKNOWN", .{});
            defer compiled.deinit();
            var program = try @import("scalar.zig").bind(alloc, compiled.expression, &.{}, &.{ .integer, .integer, .integer }, .{});
            defer program.deinit();
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "compiler column collations bind only exact native binary text domains" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "CREATE TABLE t (label text COLLATE \"C\" NOT NULL)",
        "CREATE TABLE t (label text NOT NULL COLLATE pg_catalog.\"POSIX\")",
        "CREATE TABLE t (labels text[] COLLATE \"C\")",
        "ALTER TABLE t ADD COLUMN label text COLLATE \"POSIX\" DEFAULT 'ABC'",
    }) |sql| {
        var compiled = try compile(a, sql, .{});
        defer compiled.deinit();
    }
    for ([_][]const u8{
        "CREATE TABLE t (label text COLLATE c)",
        "CREATE TABLE t (label text COLLATE \"en_US\")",
        "CREATE TABLE t (label text COLLATE tenant.\"C\")",
        "CREATE TABLE t (n integer COLLATE \"C\")",
        "ALTER TABLE t ADD COLUMN n uuid COLLATE \"C\"",
    }) |sql| try std.testing.expectError(error.UnsupportedSqlShape, compile(a, sql, .{}));
    try std.testing.expectError(error.InvalidSqlSyntax, compile(a, "CREATE TABLE t (label text COLLATE \"C\" COLLATE \"C\")", .{}));
    const Fixture = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var compiled = try compile(alloc, "CREATE TABLE t (label text COLLATE pg_catalog.\"C\" NOT NULL UNIQUE)", .{});
            defer compiled.deinit();
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Fixture.run, .{});
}

test "compiler durable defaults and stored generated columns own complete scalar trees" {
    const alloc = std.testing.allocator;
    var created = try compile(alloc, "CREATE TABLE exprs (g integer GENERATED ALWAYS AS (CASE WHEN n IS NULL THEN 0 ELSE n+1 END) STORED NOT NULL, n smallint DEFAULT (2+3) NOT NULL, label text DEFAULT lower('READY'))", .{});
    defer created.deinit();
    const columns = created.statement.create_table.columns;
    try std.testing.expect(columns[0].generated_expression.?.* == .case_when);
    try std.testing.expect(!columns[0].nullable);
    try std.testing.expect(columns[1].default_expression.?.* == .binary);
    try std.testing.expect(columns[2].default_expression.?.* == .call);
    var added = try compile(alloc, "ALTER TABLE exprs ADD COLUMN g2 bigint GENERATED ALWAYS AS (n*2) STORED NOT NULL", .{});
    defer added.deinit();
    try std.testing.expect(added.statement.catalog_ddl.schema_change.?.add_column.generated_expression.?.* == .binary);
    var changed = try compile(alloc, "ALTER TABLE exprs ALTER COLUMN n SET DEFAULT CASE WHEN true THEN 7 ELSE 9 END", .{});
    defer changed.deinit();
    try std.testing.expect(changed.statement.catalog_ddl.schema_change.?.set_default.expression.* == .case_when);
}

test "compiler persistent expression guards inspect nested parameters subqueries and references" {
    for ([_][]const u8{
        "CREATE TABLE t (n integer DEFAULT coalesce(1,$1))",
        "ALTER TABLE t ADD COLUMN n integer DEFAULT (1+$1)",
        "ALTER TABLE t ALTER COLUMN n SET DEFAULT CASE WHEN true THEN 1 ELSE $1 END",
        "CREATE TABLE t (n integer DEFAULT coalesce(1,(SELECT 2)))",
        "ALTER TABLE t ADD COLUMN n integer DEFAULT n+1",
        "CREATE TABLE t (n integer, g integer GENERATED ALWAYS AS (n+$1) STORED)",
        "CREATE TABLE t (n integer, g integer GENERATED ALWAYS AS ((SELECT 1)) STORED)",
        "CREATE TABLE t (n integer GENERATED ALWAYS AS (1) VIRTUAL)",
    }) |sql| try std.testing.expectError(error.UnsupportedSqlShape, compile(std.testing.allocator, sql, .{}));
    for ([_][]const u8{
        "CREATE TABLE t (n integer DEFAULT 1 GENERATED ALWAYS AS (2) STORED)",
        "ALTER TABLE t ADD COLUMN n integer GENERATED ALWAYS AS (1) STORED DEFAULT 2",
        "ALTER TABLE t ADD COLUMN n integer DEFAULT 1 DEFAULT 2",
    }) |sql| try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, sql, .{}));
}

test "compiler durable expression trees unwind every allocation fault" {
    const Fixture = struct {
        fn run(a: std.mem.Allocator) !void {
            var compiled = try compile(a, "CREATE TABLE exprs (g integer GENERATED ALWAYS AS (CASE WHEN n IS NULL THEN 0 ELSE n+1 END) STORED, n smallint DEFAULT (2+3), label text DEFAULT lower('READY'))", .{});
            defer compiled.deinit();
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "compiler array DDL retains PostgreSQL builtin element identity without dimension constraints" {
    const Element = @import("array_value.zig").ElementType;
    const cases = .{
        .{ .declaration = "text[]", .element = Element.text, .oid = @as(u32, 1009) },
        .{ .declaration = "smallint[]", .element = Element.int16, .oid = @as(u32, 1005) },
        .{ .declaration = "int2[7]", .element = Element.int16, .oid = @as(u32, 1005) },
        .{ .declaration = "integer[]", .element = Element.int32, .oid = @as(u32, 1007) },
        .{ .declaration = "int[][]", .element = Element.int32, .oid = @as(u32, 1007) },
        .{ .declaration = "int4[2][3]", .element = Element.int32, .oid = @as(u32, 1007) },
        .{ .declaration = "bigint[]", .element = Element.int64, .oid = @as(u32, 1016) },
        .{ .declaration = "int8[0]", .element = Element.int64, .oid = @as(u32, 1016) },
        .{ .declaration = "real[]", .element = Element.float32, .oid = @as(u32, 1021) },
        .{ .declaration = "float4[]", .element = Element.float32, .oid = @as(u32, 1021) },
        .{ .declaration = "double precision[]", .element = Element.float64, .oid = @as(u32, 1022) },
        .{ .declaration = "float8[]", .element = Element.float64, .oid = @as(u32, 1022) },
        .{ .declaration = "boolean[]", .element = Element.boolean, .oid = @as(u32, 1000) },
        .{ .declaration = "bool[]", .element = Element.boolean, .oid = @as(u32, 1000) },
        .{ .declaration = "uuid[]", .element = Element.uuid, .oid = @as(u32, 2951) },
        .{ .declaration = "jsonb[]", .element = Element.jsonb, .oid = @as(u32, 3807) },
    };
    inline for (cases) |case| {
        var create = try compile(std.testing.allocator, "CREATE TABLE arrays (payload " ++ case.declaration ++ " NOT NULL)", .{});
        defer create.deinit();
        const column = create.statement.create_table.columns[0];
        try std.testing.expectEqual(ast.ColumnType.array, column.type);
        try std.testing.expectEqual(case.element, column.element_type.?);
        try std.testing.expectEqual(case.oid, column.element_type.?.arrayOid());
        try std.testing.expect(!column.nullable);
        var alter = try compile(std.testing.allocator, "ALTER TABLE arrays ADD COLUMN payload " ++ case.declaration ++ " NOT NULL", .{});
        defer alter.deinit();
        try std.testing.expectEqualDeep(column, alter.statement.catalog_ddl.schema_change.?.add_column);
    }
}

test "compiler precise DDL descriptors retain scalar storage widths" {
    var compiled = try compile(std.testing.allocator, "CREATE TABLE widths (i integer, b bigint, f double precision, j json, a jsonb[])", .{});
    defer compiled.deinit();
    const columns = compiled.statement.create_table.columns;
    try std.testing.expectEqual(@import("array_value.zig").ElementType.int32, columns[0].element_type.?);
    try std.testing.expectEqual(@import("array_value.zig").ElementType.int64, columns[1].element_type.?);
    try std.testing.expectEqual(@import("array_value.zig").ElementType.float64, columns[2].element_type.?);
    try std.testing.expectEqual(ast.ColumnType.json, columns[3].type);
    try std.testing.expect(columns[3].element_type == null);
    try std.testing.expectEqual(ast.ColumnType.array, columns[4].type);
    for ([_][]const u8{ "smallint", "real", "int2", "float4" }) |declaration| {
        const sql = try std.fmt.allocPrint(std.testing.allocator, "CREATE TABLE widths (i {s})", .{declaration});
        defer std.testing.allocator.free(sql);
        var precise = try compile(std.testing.allocator, sql, .{});
        defer precise.deinit();
        try std.testing.expect(precise.statement.create_table.columns[0].element_type != null);
    }
    for ([_][]const u8{
        "CREATE TABLE arrays (a json[])",
        "CREATE TABLE arrays (a datetime[])",
        "CREATE TABLE arrays (a \"integer\"[])",
    }) |sql| try std.testing.expectError(error.UnsupportedSqlShape, compile(std.testing.allocator, sql, .{}));
    for ([_][]const u8{
        "CREATE TABLE arrays (a int[)",
        "CREATE TABLE arrays (a int[1.5])",
        "CREATE TABLE arrays (a int[-1])",
        "CREATE TABLE arrays (a int[2147483648])",
        "ALTER TABLE arrays ADD COLUMN a int[2",
    }) |sql| try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, sql, .{}));
}

fn arrayDdlAllocationFailureCase(alloc: std.mem.Allocator) !void {
    for ([_][]const u8{
        "CREATE TABLE arrays (a int2[2][3] NOT NULL, b jsonb[], c uuid[])",
        "ALTER TABLE arrays ADD COLUMN a double precision[][] NOT NULL",
    }) |sql| {
        var compiled = try compile(alloc, sql, .{});
        defer compiled.deinit();
    }
}

test "compiler array DDL releases all allocations on failure and retains bounded parsing" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, arrayDdlAllocationFailureCase, .{});
    try std.testing.expectError(error.SqlLimitExceeded, compile(std.testing.allocator, "CREATE TABLE arrays (a int[2][3])", .{ .max_tokens = 8 }));
}

test "compiler balances long boolean chains rather than retaining linear tree depth" {
    var source = std.ArrayList(u8).empty;
    defer source.deinit(std.testing.allocator);
    try source.appendSlice(std.testing.allocator, "SELECT * FROM t WHERE x=1");
    for (0..1023) |_| try source.appendSlice(std.testing.allocator, " AND x=1");
    var compiled = try compile(std.testing.allocator, source.items, .{ .max_depth = 16 });
    defer compiled.deinit();
    try std.testing.expect(compiled.statement.select.predicate.?.* == .conjunction);
}

test "compiler cached plan excludes comments and lexer scratch buffers" {
    var source = std.ArrayList(u8).empty;
    defer source.deinit(std.testing.allocator);
    try source.appendSlice(std.testing.allocator, "SELECT name FROM users /*");
    try source.appendNTimes(std.testing.allocator, 'x', 100_000);
    try source.appendSlice(std.testing.allocator, "*/ WHERE _id='1'");
    var compiled = try compile(std.testing.allocator, source.items, .{});
    defer compiled.deinit();
    try std.testing.expect(compiled.arena.queryCapacity() < 4096);
}

fn allocationFailureCase(allocator: std.mem.Allocator) !void {
    var compiled = try compile(allocator, "INSERT INTO \"People\" (_id,name,age) VALUES ('a', 'Ada', $1), ('b', 'Bob', 42)", .{});
    defer compiled.deinit();
    try std.testing.expectEqualStrings("People", compiled.statement.insert.table.table);
}

test "compiler releases all owned allocations on every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationFailureCase, .{});
}

test "compiler diagnostic identifies rejected trailing syntax without retained source" {
    var diagnostic: Diagnostic = .{};
    try std.testing.expectError(error.UnsupportedSqlShape, compileDiagnostic(std.testing.allocator, "SELECT * FROM t FOR UPDATE", .{}, &diagnostic));
    try std.testing.expectEqual(@as(usize, 16), diagnostic.start);
    try std.testing.expectEqual(@as(usize, 19), diagnostic.end);
    try std.testing.expectEqualStrings("unexpected trailing SQL; only one supported statement is allowed", diagnostic.message);
}

test "SQL original row-lock clauses on non-locking SQL surfaces reject before backend access" {
    // sql-0437, sql-0577, sql-0578, sql-0618: the original corpus marks
    // these row-lock forms invalid. This compiler has no row-lock syntax, so
    // reject the complete statement instead of executing its unlocked prefix.
    for ([_][]const u8{
        "SELECT u.id FROM usage_records AS u WHERE u.status = 'queued' FOR UPDATE OF archived_records",
        "UPDATE usage_records SET status = 'processing' WHERE status = 'queued' FOR SHARE RETURNING id",
        "UPDATE usage_records AS u SET status = 'processing' WHERE u.status = 'queued' FOR UPDATE OF archived_records RETURNING u.id",
        "UPDATE usage_records AS target SET status = source.status FROM source_records AS source WHERE target.id = source.id FOR UPDATE OF source RETURNING target.id",
    }) |sql| {
        try std.testing.expectError(if (std.mem.startsWith(u8, sql, "SELECT")) error.UnsupportedSqlShape else error.InvalidSqlSyntax, compile(std.testing.allocator, sql, .{}));
    }
}

test "SQL original duplicate point update target rejects before backend access" {
    // sql-0576
    try std.testing.expectError(error.DuplicateSqlAssignment, compile(std.testing.allocator, "UPDATE usage_records SET status = 'active', status = lower(status) WHERE id = 'u1'", .{}));
}

test "SQL original absent named conflict target rejects before row evaluation" {
    // sql-1487: existence/kind/timing are catalog binding decisions, not a
    // syntax rejection. An absent name must still fail before row evaluation.
    var compiled = try compile(std.testing.allocator, "INSERT INTO usage_records (id, status) VALUES ('u_bad_named', 'pending') ON CONFLICT ON CONSTRAINT usage_records_id_key DO NOTHING", .{});
    defer compiled.deinit();
    try std.testing.expectEqualStrings("usage_records_id_key", compiled.statement.insert.conflict.?.constraint_name.?);
    const Backend = struct {
        fn checkpoint(_: *anyopaque) !void {}
        fn generate(_: *anyopaque, _: std.mem.Allocator) ![]const u8 {
            return error.UnexpectedIdentityGeneration;
        }
        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: ast.Name, _: @import("catalog.zig").Action) !@import("catalog.zig").Table {
            return .{ .id = 1, .physical_name = "usage_records", .schema_version = 1, .columns = &.{ .{ .name = "id", .path = "id", .type = .string }, .{ .name = "status", .path = "status", .type = .string } } };
        }
    };
    var backend_tag: u8 = 0;
    try std.testing.expectError(error.SqlConstraintNotFound, @import("describe.zig").describe(std.testing.allocator, .{ .ptr = &backend_tag, .vtable = &.{ .resolve = Backend.resolve, .generate_row_id = Backend.generate, .scan = undefined, .mutate = undefined, .checkpoint = Backend.checkpoint } }, &compiled, &.{}));
}

test "SQL original multi-output mutation selector rejects before backend access" {
    // sql-0616, sql-0617: IN requires one projected value even when the
    // enclosing mutation would otherwise need a target table binding.
    for ([_][]const u8{
        "UPDATE usage_records SET status = 'archived' WHERE id IN (SELECT organization_id, status FROM archived_records)",
        "DELETE FROM usage_records WHERE id IN (SELECT organization_id, status FROM archived_records)",
    }) |sql| {
        try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, sql, .{}));
    }
    for ([_][]const u8{
        "SELECT (SELECT 1, 2)",
        "SELECT 1 = ANY (SELECT 1, 2)",
        "SELECT 1 IN (SELECT 1 UNION ALL SELECT 2, 3)",
    }) |sql| {
        try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, sql, .{}));
    }
}

test "SQL original unsupported catalog object commands reject before backend access" {
    // Event triggers, rules, transforms, ordinary triggers, foreign wrappers
    // and mappings, operator classes and families, and text-search objects.
    // sql-1141, sql-1142, sql-1143, sql-1144, sql-1145, sql-1146, sql-1147.
    // sql-1148, sql-1149, sql-1150, sql-1151, sql-1152, sql-1153, sql-1154.
    // sql-1155, sql-1156, sql-1157, sql-1158, sql-1159, sql-1160, sql-1161.
    // sql-1162, sql-1163, sql-1164, sql-1165, sql-1166, sql-1167, sql-1168.
    // sql-1169, sql-1170, sql-1171, sql-1172, sql-1173, sql-1174, sql-1175.
    // sql-1176, sql-1177, sql-1178, sql-1179, sql-1180, sql-1181, sql-1182.
    // sql-1185, sql-1186, sql-1187, sql-1188, sql-1189, sql-1190, sql-1191, sql-1192.
    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, @embedFile("fixtures/sql_parity_inventory.json"), .{});
    defer parsed.deinit();
    var covered: usize = 0;
    for (parsed.value.object.get("entries").?.array.items) |entry| {
        const id = entry.object.get("id").?.string;
        const ordinal = if (std.mem.startsWith(u8, id, "sql-")) std.fmt.parseInt(usize, id[4..], 10) catch continue else continue;
        if (ordinal < 1141 or ordinal > 1192 or ordinal == 1183 or ordinal == 1184) continue;
        try std.testing.expectEqualStrings("rejection", entry.object.get("source_expectation").?.string);
        var diagnostic: Diagnostic = .{};
        if (compileDiagnostic(std.testing.allocator, entry.object.get("sql").?.string, .{}, &diagnostic)) |compiled| {
            var unexpected = compiled;
            unexpected.deinit();
            std.debug.print("unexpectedly compiled {s}\n", .{id});
            return error.TestUnexpectedResult;
        } else |err| {
            try std.testing.expect(err == error.InvalidSqlSyntax or err == error.UnsupportedSqlShape);
            try std.testing.expect(diagnostic.message.len > 0);
        }
        covered += 1;
    }
    try std.testing.expectEqual(@as(usize, 50), covered);
}

test "compiler implicit projection aliases preserve clause and expression boundaries" {
    var implicit = try compile(std.testing.allocator, "SELECT id match FROM usage_records", .{});
    defer implicit.deinit();
    try std.testing.expectEqualStrings("match", implicit.statement.select.columns[0].alias.?);
    var quoted = try compile(std.testing.allocator, "SELECT id \"MixedCase\" FROM usage_records", .{});
    defer quoted.deinit();
    try std.testing.expectEqualStrings("MixedCase", quoted.statement.select.columns[0].alias.?);
    var explicit = try compile(std.testing.allocator, "SELECT id AS match FROM usage_records", .{});
    defer explicit.deinit();
    try std.testing.expectEqualStrings("match", explicit.statement.select.columns[0].alias.?);
    for ([_][]const u8{
        "SELECT id FROM usage_records WHERE id = 1",
        "SELECT id FROM usage_records GROUP BY id",
        "SELECT id FROM usage_records ORDER BY id",
        "SELECT id FROM usage_records LIMIT 2",
        "SELECT id, name FROM usage_records",
    }) |sql| {
        var statement = try compile(std.testing.allocator, sql, .{});
        defer statement.deinit();
        try std.testing.expect(statement.statement.select.columns[0].alias == null);
    }
    try std.testing.expectError(error.UnsupportedSqlShape, compile(std.testing.allocator, "SELECT id name extra FROM usage_records", .{}));
    try std.testing.expectError(error.UnsupportedSqlShape, compile(std.testing.allocator, "SELECT id 42 FROM usage_records", .{}));
}

test "compiler incomplete catalog definitions report EOF and release partial allocations" {
    const Harness = struct {
        fn run(alloc: std.mem.Allocator, sql: []const u8) !void {
            var diagnostic: Diagnostic = .{};
            var compiled = compileDiagnostic(alloc, sql, .{}, &diagnostic) catch |err| {
                if (err == error.OutOfMemory) return err;
                try std.testing.expectEqual(error.InvalidSqlSyntax, err);
                try std.testing.expect(diagnostic.start <= sql.len and diagnostic.end <= sql.len);
                try std.testing.expect(diagnostic.message.len > 0);
                return;
            };
            defer compiled.deinit();
            return error.UnexpectedCatalogAdmission;
        }
    };
    for ([_][]const u8{
        "CREATE TABLE broken (",
        "CREATE TABLE broken (n bigint",
        "CREATE TABLE broken (n bigint NULL",
        "CREATE TABLE broken (n bigint CONSTRAINT only_name",
        "CREATE TABLE broken (n bigint CONSTRAINT only_name REFERENCES",
        "CREATE TABLE broken (n bigint PRIMARY KEY,",
        "CREATE TABLE broken (CONSTRAINT only_name",
    }) |sql| try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{sql});
}

test "compiler preserves PostgreSQL expression labels before relational lowering" {
    var operators = try compile(std.testing.allocator, "SELECT status ~ 'x', status ~* 'x', status !~ 'x', status !~* 'x' FROM usage_records", .{});
    defer operators.deinit();
    for (operators.statement.select.columns[0..2]) |projection| try std.testing.expectEqualStrings("?column?", projection.alias.?);
    for (operators.statement.select.columns[2..]) |projection| try std.testing.expect(projection.alias == null);
    var statement = try compile(std.testing.allocator, "SELECT lower(name), jsonb_typeof(metadata), CASE WHEN enabled THEN 1 ELSE 0 END, upper(name) AS display FROM usage_records", .{});
    defer statement.deinit();
    for (statement.statement.select.columns, [_][]const u8{ "lower", "jsonb_typeof", "case", "display" }) |projection, label| try std.testing.expectEqualStrings(label, projection.alias.?);
    var quoted = try compile(std.testing.allocator, "SELECT id AS \"isnull\" FROM usage_records WHERE status ISNULL AND email NOTNULL", .{});
    defer quoted.deinit();
    try std.testing.expectEqualStrings("isnull", quoted.statement.select.columns[0].alias.?);
    try std.testing.expectError(error.UnsupportedSqlShape, compile(std.testing.allocator, "SELECT id FROM usage_records WHERE status \"isnull\"", .{}));
}

test "compiler ordered-set inputs remain visible to ordinary scalar visitors" {
    var corpus = try @import("parity_fixtures.zig").Corpus.init(std.testing.allocator);
    defer corpus.deinit();
    for (560..567) |ordinal| {
        var id_buffer: [8]u8 = undefined;
        const case = try corpus.get(try std.fmt.bufPrint(&id_buffer, "sql-{d:0>4}", .{ordinal}));
        var compiled = try compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        const call = compiled.statement.select.columns[1].expression.?.call;
        try std.testing.expectEqual(@as(usize, 1), call.within_group.?.orders.len);
        try std.testing.expectEqual(@as(usize, if (ordinal == 560) 1 else 2), call.args.len);
        try std.testing.expectEqualStrings(if (ordinal == 560) "status" else "amount", call.args[call.args.len - 1].column);
        try std.testing.expectEqual(ordinal == 563 or ordinal == 564, call.within_group.?.orders[0].descending);
        if (ordinal == 562) try std.testing.expectEqual(false, call.within_group.?.orders[0].nulls_first.?);
        try std.testing.expectEqual(ordinal == 564, call.filter != null);
    }
    const Fixture = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var compiled = try compile(alloc, "SELECT percentile_cont($1) WITHIN GROUP (ORDER BY x DESC NULLS FIRST) FILTER (WHERE flag) FROM t", .{});
            defer compiled.deinit();
            try std.testing.expectEqual(@as(u32, 1), compiled.parameter_count);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
    const nested = "SELECT mode() WITHIN GROUP (ORDER BY mode() WITHIN GROUP (ORDER BY mode() WITHIN GROUP (ORDER BY mode() WITHIN GROUP (ORDER BY 1))))";
    try std.testing.expectError(error.SqlLimitExceeded, compile(std.testing.allocator, nested, .{ .max_depth = 3 }));
}

test "compiler preserves keyword-named columns and quoted SQL-looking values" {
    var columns = try compile(std.testing.allocator, "SELECT count FROM t WHERE \"select\" = 'x''; DELETE FROM t; --'", .{});
    defer columns.deinit();
    try std.testing.expect(!columns.statement.select.count_all);
    try std.testing.expectEqualStrings("count", columns.statement.select.columns[0].field);
    try std.testing.expectEqualStrings("select", columns.statement.select.predicate.?.comparison.field);
    try std.testing.expectEqualStrings("x'; DELETE FROM t; --", columns.statement.select.predicate.?.comparison.value.string);
}

test "SQL COPY output retains format and read shape while guarding nonclient input" {
    for ([_][]const u8{
        "COPY usage_records (id, status) TO STDOUT WITH (FORMAT csv, FORCE_QUOTE *)",
        "COPY usage_records (id, status) TO STDOUT WITH (FORMAT text)",
        "COPY usage_records TO STDOUT WITH (FORMAT binary)",
        "COPY usage_records TO STDIN",
        "COPY (SELECT 1 AS n) TO STDOUT (FORMAT 'csv', HEADER on)",
        "COPY usage_records TO STDOUT (FORMAT text, HEADER 'false', NULL 'nil')",
    }) |sql| {
        var compiled = try compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        try std.testing.expect(compiled.statement == .copy_to);
    }
    for ([_][]const u8{
        "COPY usage_records FROM PROGRAM 'cat'",
        "COPY usage_records TO '/tmp/rows'",
        "COPY usage_records TO PROGRAM 'cat'",
        "COPY (DELETE FROM usage_records RETURNING *) TO STDOUT",
    }) |sql| try std.testing.expectError(error.UnsupportedSqlShape, compile(std.testing.allocator, sql, .{}));
    for ([_][]const u8{
        "COPY usage_records TO STDOUT (FORMAT binary, HEADER)",
        "COPY usage_records TO STDOUT (FORMAT text, QUOTE '\"')",
        "COPY usage_records TO STDOUT (FORMAT csv, DELIMITER '\"')",
        "COPY usage_records TO STDOUT (FORMAT text, FORMAT csv)",
        "COPY usage_records TO STDOUT (FORMAT csv, HEADER maybe)",
        "COPY usage_records TO STDOUT (FORMAT text, DELIMITER 'a')",
    }) |sql| try std.testing.expectError(error.SqlInvalidParameterValue, compile(std.testing.allocator, sql, .{}));
    try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, "COPY usage_records TO STDOUT WITH", .{}));
    try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, "COPY usage_records TO STDOUT (HEADER", .{}));
    try std.testing.expectError(error.DuplicateSqlColumn, compile(std.testing.allocator, "COPY usage_records(id,id) TO STDOUT", .{}));
    try std.testing.expectError(error.DuplicateSqlColumn, compile(std.testing.allocator, "COPY usage_records TO STDOUT (FORMAT csv, FORCE_QUOTE(id,id))", .{}));
    const Fixture = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var compiled = try compile(alloc, "COPY (SELECT $1::text AS a) TO STDOUT (FORMAT csv, NULL 'nil', FORCE_QUOTE(a))", .{});
            defer compiled.deinit();
            try std.testing.expectEqual(@as(u32, 1), compiled.parameter_count);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL compiler helper names are not a public function surface" {
    for ([_][]const u8{ "SELECT \"$array\"(1)", "SELECT \"$array_quantified\"(1, ARRAY[1], 99, TRUE)", "SELECT \"$validate\"(1)" }) |sql| {
        try std.testing.expectError(error.UnsupportedSqlShape, compile(std.testing.allocator, sql, .{}));
    }
}

test "SQL incomplete value expressions report syntax errors" {
    for ([_][]const u8{ "SELECT", "SELECT 1 +", "SELECT (1 +)", "INSERT INTO items (_id) VALUES (", "UPDATE items SET n =" }) |sql| {
        try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, sql, .{}));
    }
}

test "SQL search relations bind literal catalogs and explicit candidate limits" {
    for ([_][]const u8{
        "SELECT s._id FROM antfly_search('history',$1,50) AS s",
        "SELECT s._id FROM antfly_search('history',$1,limit => 50) AS s",
    }) |query| {
        var compiled = try compile(std.testing.allocator, query, .{});
        defer compiled.deinit();
        const source = compiled.statement.select.source.?.table;
        try std.testing.expectEqualStrings("history", source.name.table);
        try std.testing.expectEqualStrings("s", source.alias.?);
        try std.testing.expectEqual(@as(u32, 1), source.search.?.request.parameter);
        try std.testing.expectEqual(@as(i64, 50), source.search.?.limit.?.integer);
    }
    for ([_][]const u8{
        "SELECT * FROM antfly_search($1,$2)",
        "SELECT * FROM antfly_search('history',1)",
        "SELECT * FROM antfly_search('history','alpha',limit 50)",
    }) |query| try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, query, .{}));
}

test "SQL DROP INDEX resolves catalog ownership with an optional ON clause" {
    for ([_][]const u8{ "DROP INDEX IF EXISTS recency", "DROP INDEX IF EXISTS recency;" }) |query| {
        var compiled = try compile(std.testing.allocator, query, .{});
        defer compiled.deinit();
        const ddl = compiled.statement.catalog_ddl;
        try std.testing.expect(ddl.conditional);
        try std.testing.expect(ddl.kind == .index);
        try std.testing.expectEqualStrings("recency", ddl.index_targets[0].table);
        try std.testing.expectEqualStrings("recency", ddl.schema_change.?.drop_index);
    }
    var scoped = try compile(std.testing.allocator, "DROP INDEX recency ON public.threads", .{});
    defer scoped.deinit();
    try std.testing.expectEqualStrings("threads", scoped.statement.catalog_ddl.name.table);
    try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, "CREATE INDEX recency", .{}));
}

test "SQL view DDL compiler owns qualified read intents and PostgreSQL lifecycle clauses" {
    var source = "CREATE OR REPLACE VIEW app.public.summary(record_id) WITH (security_invoker=true, security_barrier=false) AS WITH q AS (SELECT id FROM things) SELECT id FROM q;".*;
    var compiled = try compile(std.testing.allocator, &source, .{});
    defer compiled.deinit();
    @memset(&source, 'x');
    const ddl = compiled.statement.catalog_ddl;
    try std.testing.expectEqual(.view, ddl.kind);
    try std.testing.expectEqual(.create, ddl.action);
    try std.testing.expectEqualStrings("app", ddl.name.database.?);
    try std.testing.expectEqualStrings("public", ddl.name.namespace.?);
    try std.testing.expectEqualStrings("summary", ddl.name.table);
    const definition = ddl.view_definition.?;
    try std.testing.expect(definition.replace and definition.security_invoker and !definition.security_barrier);
    try std.testing.expectEqualStrings("record_id", definition.columns[0]);
    try std.testing.expectEqualStrings("WITH q AS (SELECT id FROM things) SELECT id FROM q", definition.query_sql);
    try std.testing.expectEqual(@as(usize, 1), definition.query.ctes.len);
    var rename = try compile(std.testing.allocator, "ALTER VIEW IF EXISTS public.summary RENAME TO renamed", .{});
    defer rename.deinit();
    try std.testing.expectEqual(.rename, rename.statement.catalog_ddl.action);
    try std.testing.expect(rename.statement.catalog_ddl.conditional);
    try std.testing.expectEqualStrings("renamed", rename.statement.catalog_ddl.new_name.?);
    var drop = try compile(std.testing.allocator, "DROP VIEW IF EXISTS public.renamed CASCADE", .{});
    defer drop.deinit();
    try std.testing.expectEqual(.drop, drop.statement.catalog_ddl.action);
    try std.testing.expect(drop.statement.catalog_ddl.conditional and drop.statement.catalog_ddl.cascade);
    try std.testing.expect(drop.statement.catalog_ddl.view_definition == null);
}

test "SQL view DDL compiler rejects nonexistent PostgreSQL grammar and writable parameterized bodies" {
    for ([_][]const u8{
        "CREATE VIEW IF NOT EXISTS v AS SELECT 1",
        "CREATE OR REPLACE VIEW IF NOT EXISTS v AS SELECT 1",
        "CREATE VIEW v AS UPDATE things SET id=1",
    }) |sql| try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, sql, .{}));
    try std.testing.expectError(error.SqlInvalidParameterValue, compile(std.testing.allocator, "CREATE VIEW v WITH (security_invoker=true, security_invoker=false) AS SELECT 1", .{}));
    try std.testing.expectError(error.InvalidSqlParameter, compile(std.testing.allocator, "CREATE VIEW v AS SELECT $1", .{}));
}

test "compiler leaves anonymous constraint identities for native admission" {
    var first = try compile(std.testing.allocator, "CREATE TABLE p1 (id TEXT PRIMARY KEY)", .{});
    defer first.deinit();
    try std.testing.expect(first.statement.create_table.constraints[0].generatedConstraintName() != null);
    try std.testing.expect(!first.statement.create_table.columns[0].nullable);
    var explicit = try compile(std.testing.allocator, "CREATE TABLE foo(id TEXT CONSTRAINT exact_name PRIMARY KEY)", .{});
    defer explicit.deinit();
    try std.testing.expect(explicit.statement.create_table.constraints[0].generatedConstraintName() == null);
    try std.testing.expectEqualStrings("exact_name", explicit.statement.create_table.constraints[0].add_unique.name);
    var table_key = try compile(std.testing.allocator, "CREATE TABLE foo(id TEXT, PRIMARY KEY(id))", .{});
    defer table_key.deinit();
    try std.testing.expect(!table_key.statement.create_table.columns[0].nullable);
}

test "SQL original PostgreSQL invalid grammar retains syntax diagnostics" {
    const ids = [_][]const u8{
        "sql-0021",
        "sql-0022",
        "sql-0023",
        "sql-0024",
        "sql-0025",
        "sql-0026",
        "sql-0027",
        "sql-0028",
        "sql-0029",
        "sql-0030",
        "sql-0031",
        "sql-0032",
        "sql-0033",
        "sql-0067",
        "sql-0354",
        "sql-0355",
        "sql-0682",
        "sql-0683",
        "sql-0684",
        "sql-0686",
        "sql-0687",
        "sql-0688",
        "sql-0689",
        "sql-0690",
        "sql-0691",
        "sql-0693",
        "sql-0694",
        "sql-0698",
        "sql-0846",
        "sql-0847",
        "sql-0848",
        "sql-0849",
        "sql-0850",
        "sql-0851",
        "sql-0852",
    };
    const a = std.testing.allocator;
    var corpus = try @import("parity_fixtures.zig").Corpus.init(a);
    defer corpus.deinit();
    var failures: usize = 0;
    for (ids) |id| {
        const original = try corpus.get(id);
        if (compile(a, original.sql, .{})) |value| {
            var owned = value;
            owned.deinit();
            std.debug.print("PG grammar {s}: unexpectedly compiled\n", .{id});
            failures += 1;
        } else |err| if (err != error.InvalidSqlSyntax) {
            std.debug.print("PG grammar {s}: expected InvalidSqlSyntax got {s}\n", .{ id, @errorName(err) });
            failures += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 0), failures);
}

test "compiler SQL COMMENT COLUMN binds quoted and qualified identity without schema mutation" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "COMMENT ON COLUMN usage_records.status IS 'application state'", "COMMENT ON COLUMN public.usage_records.status IS NULL", "COMMENT ON COLUMN appdb.public.usage_records.status IS ''", "COMMENT ON COLUMN \"usage_records\".\"status\" IS 'a ''quoted'' Ω'" }) |sql| {
        var compiled = try compile(a, sql, .{});
        defer compiled.deinit();
        try std.testing.expectEqualStrings("usage_records", compiled.statement.catalog_ddl.name.table);
        try std.testing.expectEqualStrings("status", compiled.statement.catalog_ddl.comment_column.?);
        try std.testing.expectEqual(@as(@FieldType(ast.CatalogDdl, "action"), .comment), compiled.statement.catalog_ddl.action);
        try std.testing.expect(compiled.statement.catalog_ddl.schema_change == null);
    }
}

test "SQL original FK NOT VALID retains independent historical validation intent" {
    const a = std.testing.allocator;
    var compiled = try compile(a, "ALTER TABLE usage_records ADD CONSTRAINT usage_records_user_id_fkey FOREIGN KEY (user_id) REFERENCES users(id) NOT VALID;", .{});
    defer compiled.deinit();
    try std.testing.expect(compiled.statement.catalog_ddl.schema_change.?.add_foreign_key.not_valid);
    try std.testing.expectEqualStrings("usage_records_user_id_fkey", compiled.statement.catalog_ddl.schema_change.?.add_foreign_key.name);
}

test "compiler SQL COMMENT resource binds schema database and single quoted identifiers" {
    for ([_]struct { sql: []const u8, kind: @FieldType(ast.CatalogDdl, "kind"), name: []const u8 }{
        .{ .sql = "COMMENT ON SCHEMA public IS 'public schema';", .kind = .namespace, .name = "public" },
        .{ .sql = "COMMENT ON DATABASE appdb IS NULL", .kind = .database, .name = "appdb" },
        .{ .sql = "COMMENT ON SCHEMA \"scope.name\" IS 'a ''quoted'' Ω'", .kind = .namespace, .name = "scope.name" },
    }) |case| {
        var compiled = try compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        const ddl = compiled.statement.catalog_ddl;
        try std.testing.expectEqual(case.kind, ddl.kind);
        try std.testing.expectEqual(.comment, ddl.action);
        try std.testing.expectEqualStrings(case.name, ddl.name.table);
        try std.testing.expect(ddl.name.database == null and ddl.name.namespace == null and ddl.comment_column == null);
    }
    for ([_][]const u8{
        "COMMENT ON SCHEMA app.public IS 'x'",
        "COMMENT ON DATABASE app.public IS 'x'",
        "COMMENT ON SCHEMA public IS $1",
        "COMMENT ON DATABASE appdb IS lower('x')",
    }) |sql| try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, sql, .{}));
}

test "SQL original multi action ALTER rejects invalid PERIOD grammar without reinterpreting identifiers" {
    // sql-0683: parse the entire original; a supported ADD COLUMN prefix is
    // neither executable by itself nor a reason to hide a later syntax error.
    var corpus = try @import("parity_fixtures.zig").Corpus.init(std.testing.allocator);
    defer corpus.deinit();
    const entry = try corpus.get("sql-0683");
    var diagnostic: Diagnostic = .{};
    try std.testing.expectError(error.InvalidSqlSyntax, compileDiagnostic(std.testing.allocator, entry.sql, .{}, &diagnostic));
    try std.testing.expectEqualStrings("PERIOD", entry.sql[diagnostic.start..diagnostic.end]);
    for ([_][]const u8{
        "ALTER TABLE t ADD COLUMN note text DEFAULT 'ADD PERIOD FOR x(a,b)', ADD COLUMN n integer",
        "ALTER TABLE t ADD COLUMN n integer CHECK (n > 0 /* ADD PERIOD FOR x(a,b) */), ADD COLUMN m integer",
        "ALTER TABLE t ADD COLUMN n integer, ADD COLUMN \"period\" text",
    }) |sql| try std.testing.expectError(error.UnsupportedSqlShape, compile(std.testing.allocator, sql, .{}));
}

test "SQL range temporal uniqueness retains its final column and rejects malformed clauses" {
    const a = std.testing.allocator;
    var corpus = try @import("parity_fixtures.zig").Corpus.init(a);
    defer corpus.deinit();
    var original = try compile(a, (try corpus.get("sql-0685")).sql, .{});
    defer original.deinit();
    const constraint = original.statement.create_table.constraints[0].add_unique;
    try std.testing.expectEqualStrings("valid_at", constraint.without_overlaps_column.?);
    try std.testing.expectEqual(@as(usize, 2), constraint.columns.len);
    var distinct = try compile(a, "CREATE TABLE t(id text,v numrange,UNIQUE(id,v),UNIQUE(id,v WITHOUT OVERLAPS))", .{});
    defer distinct.deinit();
    try std.testing.expectEqual(@as(usize, 2), distinct.statement.create_table.constraints.len);
    try std.testing.expect(distinct.statement.create_table.constraints[0].add_unique.without_overlaps_column == null);
    try std.testing.expectEqualStrings("v", distinct.statement.create_table.constraints[1].add_unique.without_overlaps_column.?);

    try std.testing.expectError(error.InvalidSqlSyntax, compile(a, "CREATE TABLE t (id text, v numrange, PRIMARY KEY(id,v WITHOUT))", .{}));
    try std.testing.expectError(error.InvalidSqlSyntax, compile(a, "CREATE TABLE t (id text, v numrange, PRIMARY KEY(id,v WITHOUT OVERLAPS,id))", .{}));
}

test "compiler preserves PostgreSQL cast column labels before lowering" {
    var statement = try compile(std.testing.allocator, "SELECT id::text, q.id::text::text, CAST(q.id AS text), lower(q.id)::text, q.id::text AS explicit_name FROM usage_records q", .{});
    defer statement.deinit();
    for (statement.statement.select.columns, [_][]const u8{ "id", "id", "id", "lower", "explicit_name" }) |projection, name|
        try std.testing.expectEqualStrings(name, projection.alias.?);
}

test "SQL owned sequence defaults parse serial aliases and identity options as private creation intent" {
    const a = std.testing.allocator;
    var corpus = try @import("parity_fixtures.zig").Corpus.init(a);
    defer corpus.deinit();
    for ([_][]const u8{ "sql-0318", "sql-0319", "sql-0320" }, 0..) |id, index| {
        const original = try corpus.get(id);
        var compiled = try compile(a, original.sql, .{});
        defer compiled.deinit();
        const table = compiled.statement.create_table;
        try std.testing.expectEqual(@as(usize, 2), table.columns.len);
        const column = table.columns[0];
        try std.testing.expectEqualStrings("id", column.name);
        try std.testing.expect(!column.nullable);
        try std.testing.expectEqual(@as(?@import("array_value.zig").ElementType, .int64), column.element_type);
        const intent = column.sequence orelse return error.MissingOwnedSequenceIntent;
        try std.testing.expectEqual(([_]@import("../system_catalog/column_sequences.zig").Generation{ .serial, .by_default, .always })[index], intent.generation);
        try std.testing.expectEqual(.unchanged, intent.options.owned_by);
        if (index == 2) {
            try std.testing.expectEqual(@as(?i64, 100), intent.options.start);
            try std.testing.expectEqual(@as(?i64, 10), intent.options.increment);
            try std.testing.expectEqual(@as(?u64, 4), intent.options.cache);
            try std.testing.expectEqual(@as(?bool, false), intent.options.cycle);
        }
    }
    for ([_][]const u8{ "smallserial", "serial2", "serial", "serial4", "bigserial", "serial8" }, 0..) |alias, index| {
        const text = try std.fmt.allocPrint(a, "CREATE TABLE t (id {s})", .{alias});
        defer a.free(text);
        var compiled = try compile(a, text, .{});
        defer compiled.deinit();
        try std.testing.expectEqual(([_]@import("../system_catalog/sequences.zig").Integer{ .int16, .int32, .int64 })[index / 2], compiled.statement.create_table.columns[0].sequence.?.options.integer.?);
    }
    var stored = try compile(a, "CREATE TABLE t (id bigint NULL GENERATED ALWAYS AS (7) STORED)", .{});
    defer stored.deinit();
    try std.testing.expect(stored.statement.create_table.columns[0].sequence == null);
    try std.testing.expect(stored.statement.create_table.columns[0].nullable);
    try std.testing.expectError(error.InvalidSqlSyntax, compile(a, "CREATE TABLE t (id bigserial DEFAULT 7)", .{}));
    try std.testing.expectError(error.InvalidSqlSyntax, compile(a, "CREATE TABLE t (id bigserial NULL)", .{}));
    try std.testing.expectError(error.InvalidSqlSyntax, compile(a, "CREATE TABLE t (id bigint NULL GENERATED ALWAYS AS IDENTITY)", .{}));
}

test "compiler implicit INSERT budgets DEFAULT cells and rejects ragged rows" {
    try std.testing.expectError(error.SqlLimitExceeded, compile(std.testing.allocator, "INSERT INTO t VALUES(DEFAULT,DEFAULT,DEFAULT,DEFAULT)", .{ .max_nodes = 2 }));
    try std.testing.expectError(error.InvalidSqlSyntax, compile(std.testing.allocator, "INSERT INTO t VALUES(1),(2,3)", .{}));
    var compiled = try compile(std.testing.allocator, "INSERT INTO t VALUES(DEFAULT),(DEFAULT)", .{});
    defer compiled.deinit();
    try std.testing.expect(compiled.statement.insert.implicit_columns);
    try std.testing.expectEqual(@as(usize, 2), compiled.statement.insert.rows.len);
    try std.testing.expect(compiled.statement.insert.isDefault(0, 0));
    try std.testing.expect(compiled.statement.insert.isDefault(1, 0));
}

test "SQL COPY input advanced options retain owned column domains and PostgreSQL option failures" {
    const a = std.testing.allocator;
    var compiled = try compile(a, "COPY t(id,status,amount) FROM STDIN (FORMAT csv, HEADER match, FORCE_NULL(status), FORCE_NOT_NULL(status), DEFAULT 'n/a', ON_ERROR ignore, REJECT_LIMIT 10, LOG_VERBOSITY silent)", .{});
    defer compiled.deinit();
    const options = compiled.statement.copy_from.options;
    try std.testing.expect(options.header and options.header_match);
    try std.testing.expectEqualStrings("status", options.force_null_columns[0]);
    try std.testing.expectEqualStrings("n/a", options.default_string.?);
    try std.testing.expectEqual(.ignore, options.on_error);
    try std.testing.expectEqual(@as(?u64, 10), options.reject_limit);
    try std.testing.expectEqual(.silent, options.log_verbosity);
    for ([_][]const u8{
        "COPY t FROM STDIN (FORMAT csv, OIDS false)",
        "COPY t FROM STDIN (FORMAT binary, ON_ERROR ignore)",
    }) |sql| try std.testing.expectError(error.InvalidSqlSyntax, compile(a, sql, .{}));
    for ([_][]const u8{
        "COPY t FROM STDIN (FORMAT text, FORCE_NULL(id))",
        "COPY t FROM STDIN (FORMAT csv, FORCE_QUOTE *)",
        "COPY t TO STDOUT (FORMAT csv, DEFAULT 'd')",
        "COPY t TO STDOUT (FORMAT csv, HEADER match)",
        "COPY t FROM STDIN (FORMAT csv, DEFAULT '')",
    }) |sql| try std.testing.expectError(error.UnsupportedSqlShape, compile(a, sql, .{}));
    try std.testing.expectError(error.DuplicateSqlColumn, compile(a, "COPY t FROM STDIN (FORMAT csv, FORCE_NULL(id,id))", .{}));
    for ([_][]const u8{
        "COPY t TO STDOUT (ON_ERROR stop)",
        "COPY t TO STDOUT (FORMAT csv, FORCE_NULL(id))",
        "COPY t FROM STDIN (REJECT_LIMIT 1)",
        "COPY t FROM STDIN (ON_ERROR ignore, REJECT_LIMIT 0)",
    }) |sql| try std.testing.expectError(error.SqlInvalidParameterValue, compile(a, sql, .{}));
}
