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

fn classifyWithWriteStatement(tokens: []const Token) ?SqlWriteStatementKind {
    var index: usize = 1;
    if (parser.matchKeyword(tokens, &index, "recursive")) return null;

    while (true) {
        if (index >= tokens.len or tokens[index].kind != .identifier) return null;
        index += 1;
        if (index < tokens.len and tokens[index].kind == .lparen) {
            index = (parser.findMatchingRParenIndex(tokens, index) orelse return null) + 1;
        }
        if (!parser.matchKeyword(tokens, &index, "as")) return null;
        if (!consumeCteMaterializationHint(tokens, &index)) return null;
        if (index >= tokens.len or tokens[index].kind != .lparen) return null;
        index = (parser.findMatchingRParenIndex(tokens, index) orelse return null) + 1;
        if (index < tokens.len and tokens[index].kind == .comma) {
            index += 1;
            continue;
        }
        break;
    }

    if (index >= tokens.len or tokens[index].kind != .identifier) return null;
    if (std.ascii.eqlIgnoreCase(tokens[index].text, "insert")) return .insert_source;
    if (std.ascii.eqlIgnoreCase(tokens[index].text, "update")) return .update;
    if (std.ascii.eqlIgnoreCase(tokens[index].text, "delete")) return .delete;
    if (std.ascii.eqlIgnoreCase(tokens[index].text, "merge")) return .merge;
    if (std.ascii.eqlIgnoreCase(tokens[index].text, "truncate")) return .truncate;
    return null;
}

fn consumeCteMaterializationHint(tokens: []const Token, index: *usize) bool {
    if (parser.matchKeyword(tokens, index, "materialized")) return true;
    if (parser.matchKeyword(tokens, index, "not")) {
        return parser.matchKeyword(tokens, index, "materialized");
    }
    return true;
}

fn firstIdentifier(tokens: []const Token) ?[]const u8 {
    if (tokens.len == 0) return null;
    if (tokens[0].kind != .identifier) return null;
    return tokens[0].text;
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
