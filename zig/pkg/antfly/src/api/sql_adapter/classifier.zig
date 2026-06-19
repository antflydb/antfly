// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

const lexer = @import("lexer.zig");
const parser = @import("parser.zig");
const token_mod = @import("token.zig");

pub const Token = token_mod.Token;

pub const SqlStatementFamily = enum {
    select,
    insert,
    update,
    delete,
    truncate,
    merge,
    with,
    ddl,
};

pub const SqlWriteStatementKind = enum {
    insert,
    insert_source,
    update,
    delete,
    truncate,
    merge,
};

pub const SqlPreparedStatementSubjectKind = enum {
    read,
    write,
    ddl,
};

pub const SqlPreparedStatementStatementKind = enum {
    read,
    insert,
    insert_source,
    update,
    delete,
    truncate,
    merge,
    ddl,
};

pub fn classifyStatementFamily(tokens: []const Token) ?SqlStatementFamily {
    const first = firstIdentifier(tokens) orelse return null;
    if (std.ascii.eqlIgnoreCase(first, "select")) return .select;
    if (std.ascii.eqlIgnoreCase(first, "insert")) return .insert;
    if (std.ascii.eqlIgnoreCase(first, "update")) return .update;
    if (std.ascii.eqlIgnoreCase(first, "delete")) return .delete;
    if (std.ascii.eqlIgnoreCase(first, "truncate")) return .truncate;
    if (std.ascii.eqlIgnoreCase(first, "merge")) return .merge;
    if (std.ascii.eqlIgnoreCase(first, "with")) return .with;
    return .ddl;
}

pub fn classifyWriteStatement(tokens: []const Token) ?SqlWriteStatementKind {
    return switch (classifyStatementFamily(tokens) orelse return null) {
        .insert => .insert,
        .with => classifyWithWriteStatement(tokens),
        .update => .update,
        .delete => .delete,
        .truncate => .truncate,
        .merge => .merge,
        .select, .ddl => null,
    };
}

pub fn classifyPreparedStatementSubjectKind(tokens: []const Token, start: usize) ?SqlPreparedStatementSubjectKind {
    return preparedStatementSubjectKindFromStatementKind(classifyPreparedStatementStatementKind(tokens, start) orelse return null);
}

pub fn classifyPreparedStatementStatementKind(tokens: []const Token, start: usize) ?SqlPreparedStatementStatementKind {
    if (statementStartsAt(tokens, start, "select")) return .read;
    if (statementStartsAt(tokens, start, "with")) {
        const final_index = withFinalStatementIndex(tokens[start..]) orelse return null;
        if (std.ascii.eqlIgnoreCase(tokens[start + final_index].text, "select")) return .read;
        if (withFinalStatementWriteKind(tokens[start + final_index].text)) |kind| return preparedStatementKindFromWriteKind(kind);
        return null;
    }
    if (statementStartsAt(tokens, start, "insert")) return .insert;
    if (statementStartsAt(tokens, start, "update")) return .update;
    if (statementStartsAt(tokens, start, "delete")) return .delete;
    if (statementStartsAt(tokens, start, "truncate")) return .truncate;
    if (statementStartsAt(tokens, start, "merge")) return .merge;
    if (statementStartsAt(tokens, start, "create") or
        statementStartsAt(tokens, start, "alter") or
        statementStartsAt(tokens, start, "drop"))
    {
        return .ddl;
    }
    return null;
}

pub fn preparedStatementSubjectKindFromStatementKind(kind: SqlPreparedStatementStatementKind) SqlPreparedStatementSubjectKind {
    return switch (kind) {
        .read => .read,
        .insert,
        .insert_source,
        .update,
        .delete,
        .truncate,
        .merge,
        => .write,
        .ddl => .ddl,
    };
}

fn preparedStatementKindFromWriteKind(kind: SqlWriteStatementKind) SqlPreparedStatementStatementKind {
    return switch (kind) {
        .insert => .insert,
        .insert_source => .insert_source,
        .update => .update,
        .delete => .delete,
        .truncate => .truncate,
        .merge => .merge,
    };
}

fn classifyWithWriteStatement(tokens: []const Token) ?SqlWriteStatementKind {
    const index = withFinalStatementIndex(tokens) orelse return null;
    return withFinalStatementWriteKind(tokens[index].text);
}

fn withFinalStatementIndex(tokens: []const Token) ?usize {
    var index: usize = 1;
    if (parser.matchKeyword(tokens, &index, "recursive")) return null;

    while (true) {
        if (index >= tokens.len or tokens[index].kind != .identifier) return null;
        index += 1;
        if (index < tokens.len and tokens[index].kind == .lparen) {
            index = (parser.findMatchingRParenIndex(tokens, index) orelse return null) + 1;
        }
        if (!parser.matchKeyword(tokens, &index, "as")) return null;
        parser.consumeCteMaterializationHint(tokens, &index) catch return null;
        if (index >= tokens.len or tokens[index].kind != .lparen) return null;
        index = (parser.findMatchingRParenIndex(tokens, index) orelse return null) + 1;
        if (index < tokens.len and tokens[index].kind == .comma) {
            index += 1;
            continue;
        }
        break;
    }

    if (index >= tokens.len or tokens[index].kind != .identifier) return null;
    return index;
}

fn withFinalStatementWriteKind(keyword: []const u8) ?SqlWriteStatementKind {
    if (std.ascii.eqlIgnoreCase(keyword, "insert")) return .insert_source;
    if (std.ascii.eqlIgnoreCase(keyword, "update")) return .update;
    if (std.ascii.eqlIgnoreCase(keyword, "delete")) return .delete;
    if (std.ascii.eqlIgnoreCase(keyword, "merge")) return .merge;
    if (std.ascii.eqlIgnoreCase(keyword, "truncate")) return .truncate;
    return null;
}

fn firstIdentifier(tokens: []const Token) ?[]const u8 {
    if (tokens.len == 0) return null;
    if (tokens[0].kind != .identifier) return null;
    return tokens[0].text;
}

fn statementStartsAt(tokens: []const Token, start: usize, keyword: []const u8) bool {
    if (start >= tokens.len) return false;
    if (tokens[start].kind != .identifier) return false;
    return std.ascii.eqlIgnoreCase(tokens[start].text, keyword);
}

test "sql adapter classifier identifies write statement families" {
    const alloc = std.testing.allocator;
    const cases = [_]struct {
        sql: []const u8,
        expected: SqlWriteStatementKind,
    }{
        .{ .sql = "INSERT INTO usage_records(id) VALUES ('u1')", .expected = .insert },
        .{ .sql = "WITH source_rows AS (SELECT id FROM usage_records) INSERT INTO archive(id) SELECT id FROM source_rows", .expected = .insert_source },
        .{ .sql = "WITH source_rows AS MATERIALIZED (SELECT id FROM usage_records) UPDATE usage_records SET status = 'done' WHERE id IN (SELECT id FROM source_rows)", .expected = .update },
        .{ .sql = "WITH source_rows AS NOT MATERIALIZED (SELECT id FROM usage_records) DELETE FROM usage_records WHERE id IN (SELECT id FROM source_rows)", .expected = .delete },
        .{ .sql = "WITH source_rows AS (SELECT id FROM usage_records) MERGE INTO usage_records USING source_rows ON usage_records.id = source_rows.id WHEN MATCHED THEN UPDATE SET status = 'done'", .expected = .merge },
        .{ .sql = "UPDATE usage_records SET status = 'done'", .expected = .update },
        .{ .sql = "DELETE FROM usage_records", .expected = .delete },
        .{ .sql = "TRUNCATE usage_records", .expected = .truncate },
        .{ .sql = "MERGE INTO usage_records USING source_rows ON usage_records.id = source_rows.id WHEN MATCHED THEN UPDATE SET status = source_rows.status", .expected = .merge },
    };
    for (cases) |case| {
        var tokens = try lexer.tokenizeAlloc(alloc, case.sql);
        defer lexer.freeTokens(alloc, &tokens);
        try std.testing.expectEqual(case.expected, classifyWriteStatement(tokens.items).?);
    }
}

test "sql adapter classifier rejects non-write and non-token statements" {
    const alloc = std.testing.allocator;
    var select_tokens = try lexer.tokenizeAlloc(alloc, "SELECT id FROM usage_records");
    defer lexer.freeTokens(alloc, &select_tokens);
    try std.testing.expect(classifyWriteStatement(select_tokens.items) == null);

    var ddl_tokens = try lexer.tokenizeAlloc(alloc, "CREATE TABLE usage_records(id text)");
    defer lexer.freeTokens(alloc, &ddl_tokens);
    try std.testing.expectEqual(SqlStatementFamily.ddl, classifyStatementFamily(ddl_tokens.items).?);
    try std.testing.expect(classifyWriteStatement(ddl_tokens.items) == null);

    var with_select_tokens = try lexer.tokenizeAlloc(alloc, "WITH source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows");
    defer lexer.freeTokens(alloc, &with_select_tokens);
    try std.testing.expectEqual(SqlStatementFamily.with, classifyStatementFamily(with_select_tokens.items).?);
    try std.testing.expect(classifyWriteStatement(with_select_tokens.items) == null);

    var recursive_tokens = try lexer.tokenizeAlloc(alloc, "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records) UPDATE usage_records SET status = 'done'");
    defer lexer.freeTokens(alloc, &recursive_tokens);
    try std.testing.expect(classifyWriteStatement(recursive_tokens.items) == null);

    try std.testing.expect(classifyStatementFamily(&.{}) == null);
}

test "sql adapter classifier identifies prepared statement subject families" {
    const alloc = std.testing.allocator;
    const cases = [_]struct {
        sql: []const u8,
        start_keyword: []const u8,
        expected: SqlPreparedStatementSubjectKind,
    }{
        .{ .sql = "PREPARE usage_plan AS SELECT id FROM usage_records", .start_keyword = "select", .expected = .read },
        .{ .sql = "PREPARE usage_plan AS WITH source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows", .start_keyword = "with", .expected = .read },
        .{ .sql = "PREPARE usage_plan AS WITH source_rows AS (SELECT id FROM usage_records) INSERT INTO archived_records(id) SELECT id FROM source_rows", .start_keyword = "with", .expected = .write },
        .{ .sql = "PREPARE usage_plan AS WITH source_rows AS (SELECT id FROM usage_records) UPDATE usage_records SET status = 'done' WHERE id IN (SELECT id FROM source_rows)", .start_keyword = "with", .expected = .write },
        .{ .sql = "PREPARE usage_plan AS WITH source_rows AS (SELECT id FROM usage_records) DELETE FROM usage_records WHERE id IN (SELECT id FROM source_rows)", .start_keyword = "with", .expected = .write },
        .{ .sql = "PREPARE usage_plan AS WITH source_rows AS (SELECT id FROM usage_records) MERGE INTO usage_records USING source_rows ON usage_records.id = source_rows.id WHEN MATCHED THEN UPDATE SET status = source_rows.status", .start_keyword = "with", .expected = .write },
        .{ .sql = "PREPARE usage_plan AS INSERT INTO usage_records(id) VALUES ($1)", .start_keyword = "insert", .expected = .write },
        .{ .sql = "PREPARE usage_plan AS UPDATE usage_records SET status = $1", .start_keyword = "update", .expected = .write },
        .{ .sql = "PREPARE usage_plan AS DELETE FROM usage_records", .start_keyword = "delete", .expected = .write },
        .{ .sql = "PREPARE usage_plan AS TRUNCATE usage_records", .start_keyword = "truncate", .expected = .write },
        .{ .sql = "PREPARE usage_plan AS MERGE INTO usage_records USING source_records ON usage_records.id = source_records.id WHEN MATCHED THEN UPDATE SET status = source_records.status", .start_keyword = "merge", .expected = .write },
        .{ .sql = "PREPARE usage_plan AS CREATE TABLE usage_records(id text)", .start_keyword = "create", .expected = .ddl },
        .{ .sql = "PREPARE usage_plan AS ALTER TABLE usage_records ADD COLUMN status text", .start_keyword = "alter", .expected = .ddl },
        .{ .sql = "PREPARE usage_plan AS DROP TABLE usage_records", .start_keyword = "drop", .expected = .ddl },
    };
    for (cases) |case| {
        var tokens = try lexer.tokenizeAlloc(alloc, case.sql);
        defer lexer.freeTokens(alloc, &tokens);
        const start = parser.findTopLevelKeyword(tokens.items, case.start_keyword).?;
        try std.testing.expectEqual(case.expected, classifyPreparedStatementSubjectKind(tokens.items, start).?);
    }

    var explain_tokens = try lexer.tokenizeAlloc(alloc, "PREPARE usage_plan AS EXPLAIN SELECT id FROM usage_records");
    defer lexer.freeTokens(alloc, &explain_tokens);
    const explain_start = parser.findTopLevelKeyword(explain_tokens.items, "explain").?;
    try std.testing.expect(classifyPreparedStatementSubjectKind(explain_tokens.items, explain_start) == null);

    var recursive_tokens = try lexer.tokenizeAlloc(alloc, "PREPARE usage_plan AS WITH RECURSIVE source_rows AS (SELECT id FROM usage_records) UPDATE usage_records SET status = 'done'");
    defer lexer.freeTokens(alloc, &recursive_tokens);
    const recursive_start = parser.findTopLevelKeyword(recursive_tokens.items, "with").?;
    try std.testing.expect(classifyPreparedStatementSubjectKind(recursive_tokens.items, recursive_start) == null);
}
