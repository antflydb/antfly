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
pub const TokenKeyword = token_mod.TokenKeyword;

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

pub const SqlReadStatementKind = enum {
    query,
    set_operation,
    recursive_cte,
    aggregate,
    join,
    lateral,
    window,
};

pub const SqlWriteStatementKind = enum {
    insert,
    insert_source,
    update,
    update_joined_source,
    delete,
    delete_joined_source,
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
    if (first.keyword) |keyword| {
        return switch (keyword) {
            .select => .select,
            .insert => .insert,
            .update => .update,
            .delete => .delete,
            .truncate => .truncate,
            .merge => .merge,
            .with => .with,
            else => .ddl,
        };
    }
    return if (first.kind == .identifier) .ddl else null;
}

pub fn classifyWriteStatement(tokens: []const Token) ?SqlWriteStatementKind {
    return switch (classifyStatementFamily(tokens) orelse return null) {
        .insert => classifyInsertStatement(tokens, 0),
        .with => classifyWithWriteStatement(tokens),
        .update => classifyUpdateStatement(tokens, 0),
        .delete => classifyDeleteStatement(tokens, 0),
        .truncate => .truncate,
        .merge => .merge,
        .select, .ddl => null,
    };
}

pub fn classifyRecursiveWriteStatement(tokens: []const Token) ?SqlWriteStatementKind {
    const index = withFinalStatementIndex(tokens, .{ .allow_recursive = true }) orelse return null;
    return withFinalStatementWriteKind(tokens[index..]);
}

pub fn classifyReadStatement(tokens: []const Token) ?SqlReadStatementKind {
    const family = classifyStatementFamily(tokens) orelse return null;
    const statement_start = switch (family) {
        .select => @as(usize, 0),
        .with => withFinalStatementIndex(tokens, .{ .allow_recursive = true }) orelse return null,
        .insert, .update, .delete, .truncate, .merge, .ddl => return null,
    };
    if (!statementStartsAt(tokens, statement_start, "select")) return null;
    if (family == .with and tokens.len > 1 and keywordAt(tokens, 1, "recursive")) return .recursive_cte;

    const statement = tokens[statement_start..];
    if (readHasTopLevelSetOperation(statement)) return .set_operation;
    if (readHasTopLevelKeyword(statement, "lateral")) return .lateral;
    if (readHasTopLevelKeyword(statement, "over")) return .window;
    if (readIsDistinctOnShape(statement)) return .query;
    if (readHasAggregateShape(statement)) return .aggregate;
    if (readHasTopLevelKeyword(statement, "join")) return .join;
    return .query;
}

pub fn classifyPreparedStatementSubjectKind(tokens: []const Token, start: usize) ?SqlPreparedStatementSubjectKind {
    return preparedStatementSubjectKindFromStatementKind(classifyPreparedStatementStatementKind(tokens, start) orelse return null);
}

pub fn classifyPreparedStatementStatementKind(tokens: []const Token, start: usize) ?SqlPreparedStatementStatementKind {
    if (statementStartsAt(tokens, start, "select")) return .read;
    if (statementStartsAt(tokens, start, "with")) {
        const with_tokens = tokens[start..];
        const final_index = withFinalStatementIndex(with_tokens, .{ .allow_recursive = true }) orelse return null;
        if (with_tokens[final_index].isKeyword(.select)) return .read;
        if (withFinalStatementWriteKind(with_tokens[final_index..])) |kind| return preparedStatementKindFromWriteKind(kind);
        return null;
    }
    if (classifyInsertStatement(tokens, start)) |kind| return preparedStatementKindFromWriteKind(kind);
    if (classifyUpdateStatement(tokens, start)) |kind| return preparedStatementKindFromWriteKind(kind);
    if (classifyDeleteStatement(tokens, start)) |kind| return preparedStatementKindFromWriteKind(kind);
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
        .update_joined_source => .update,
        .delete => .delete,
        .delete_joined_source => .delete,
        .truncate => .truncate,
        .merge => .merge,
    };
}

fn classifyWithWriteStatement(tokens: []const Token) ?SqlWriteStatementKind {
    const index = withFinalStatementIndex(tokens, .{}) orelse return null;
    return withFinalStatementWriteKind(tokens[index..]);
}

const WithFinalStatementOptions = struct {
    allow_recursive: bool = false,
};

fn withFinalStatementIndex(tokens: []const Token, options: WithFinalStatementOptions) ?usize {
    var index: usize = 1;
    if (parser.matchKeyword(tokens, &index, "recursive") and !options.allow_recursive) return null;

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

fn withFinalStatementWriteKind(tokens: []const Token) ?SqlWriteStatementKind {
    if (tokens.len == 0 or tokens[0].kind != .identifier) return null;
    return switch (tokens[0].keyword orelse return null) {
        .insert => classifyInsertStatement(tokens, 0),
        .update => classifyUpdateStatement(tokens, 0),
        .delete => classifyDeleteStatement(tokens, 0),
        .merge => .merge,
        .truncate => .truncate,
        else => null,
    };
}

fn classifyInsertStatement(tokens: []const Token, start: usize) ?SqlWriteStatementKind {
    if (!statementStartsAt(tokens, start, "insert")) return null;
    const statement = tokens[start..];
    const select_index = parser.findTopLevelKeyword(statement, "select");
    const values_index = parser.findTopLevelKeyword(statement, "values");
    const default_index = parser.findTopLevelKeyword(statement, "default");
    if (select_index) |select_pos| {
        if (values_index == null or select_pos < values_index.?) {
            if (default_index == null or select_pos < default_index.?) return .insert_source;
        }
    }
    return .insert;
}

fn classifyUpdateStatement(tokens: []const Token, start: usize) ?SqlWriteStatementKind {
    if (!statementStartsAt(tokens, start, "update")) return null;
    const statement = tokens[start..];
    const set_index = parser.findTopLevelKeyword(statement, "set") orelse return .update;
    if (parser.findTopLevelKeywordFromIndex(statement, set_index + 1, "from")) |from_index| {
        const stop_index = firstTopLevelKeywordIndex(statement, from_index + 1, &.{ "where", "order", "limit", "offset", "fetch", "for", "returning" }) orelse statement.len;
        if (from_index < stop_index) return .update_joined_source;
    }
    return if (statementHasWhereSemijoinSubquery(statement, set_index + 1)) .update_joined_source else .update;
}

fn classifyDeleteStatement(tokens: []const Token, start: usize) ?SqlWriteStatementKind {
    if (!statementStartsAt(tokens, start, "delete")) return null;
    const statement = tokens[start..];
    const from_index = parser.findTopLevelKeyword(statement, "from") orelse return .delete;
    if (parser.findTopLevelKeywordFromIndex(statement, from_index + 1, "using")) |using_index| {
        const stop_index = firstTopLevelKeywordIndex(statement, from_index + 1, &.{ "where", "order", "limit", "offset", "fetch", "for", "returning" }) orelse statement.len;
        if (using_index < stop_index) return .delete_joined_source;
    }
    return if (statementHasWhereSemijoinSubquery(statement, from_index + 1)) .delete_joined_source else .delete;
}

fn firstTopLevelKeywordIndex(tokens: []const Token, start: usize, keywords: []const []const u8) ?usize {
    var first: ?usize = null;
    for (keywords) |keyword| {
        if (parser.findTopLevelKeywordFromIndex(tokens, start, keyword)) |index| {
            if (first == null or index < first.?) first = index;
        }
    }
    return first;
}

fn statementHasWhereSemijoinSubquery(tokens: []const Token, start: usize) bool {
    const where_index = parser.findTopLevelKeywordFromIndex(tokens, start, "where") orelse return false;
    const stop_index = firstTopLevelKeywordIndex(tokens, where_index + 1, &.{ "order", "limit", "offset", "fetch", "for", "returning" }) orelse tokens.len;
    return tokenRangeHasTopLevelSemijoinSubquery(tokens[where_index + 1 .. stop_index]);
}

fn tokenRangeHasTopLevelSemijoinSubquery(tokens: []const Token) bool {
    var depth: usize = 0;
    for (tokens, 0..) |token, index| {
        switch (token.kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth > 0) depth -= 1;
            },
            .identifier => if (depth == 0) {
                if ((token.matchesKeyword("in") or token.matchesKeyword("exists")) and nextParenContainsSelect(tokens, index + 1)) return true;
            },
            else => {},
        }
    }
    return false;
}

fn nextParenContainsSelect(tokens: []const Token, start: usize) bool {
    var index = start;
    while (index < tokens.len and tokens[index].kind == .comma) : (index += 1) {}
    if (index >= tokens.len or tokens[index].kind != .lparen) return false;
    if (index + 1 >= tokens.len or !tokens[index + 1].matchesKeyword("select")) return false;
    return parser.findMatchingRParenIndex(tokens, index) != null;
}

fn keywordAt(tokens: []const Token, index: usize, keyword: []const u8) bool {
    return index < tokens.len and tokens[index].matchesKeyword(keyword);
}

fn firstIdentifier(tokens: []const Token) ?Token {
    if (tokens.len == 0) return null;
    if (tokens[0].kind != .identifier) return null;
    return tokens[0];
}

fn statementStartsAt(tokens: []const Token, start: usize, keyword: []const u8) bool {
    return keywordAt(tokens, start, keyword);
}

fn readHasTopLevelSetOperation(tokens: []const Token) bool {
    return readHasTopLevelKeyword(tokens, "union") or
        readHasTopLevelKeyword(tokens, "intersect") or
        readHasTopLevelKeyword(tokens, "except");
}

fn readHasTopLevelKeyword(tokens: []const Token, keyword: []const u8) bool {
    var depth: usize = 0;
    for (tokens) |token| {
        switch (token.kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth > 0) depth -= 1;
            },
            .identifier => if (depth == 0 and token.matchesKeyword(keyword)) return true,
            else => {},
        }
    }
    return false;
}

fn readHasAggregateShape(tokens: []const Token) bool {
    if (tokens.len > 1 and tokens[0].isKeyword(.select) and tokens[1].isKeyword(.distinct) and !readIsDistinctOnShape(tokens)) {
        return true;
    }

    var depth: usize = 0;
    for (tokens, 0..) |token, index| {
        switch (token.kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth > 0) depth -= 1;
            },
            .identifier => if (depth == 0) {
                if (std.ascii.eqlIgnoreCase(token.text, "group") or
                    std.ascii.eqlIgnoreCase(token.text, "having"))
                {
                    return true;
                }
                if (index + 1 < tokens.len and tokens[index + 1].kind == .lparen and sqlAggregateFunctionName(token.text)) {
                    return true;
                }
            },
            else => {},
        }
    }
    return false;
}

fn readIsDistinctOnShape(tokens: []const Token) bool {
    return tokens.len > 2 and tokens[0].isKeyword(.select) and tokens[1].isKeyword(.distinct) and tokens[2].isKeyword(.on);
}

fn sqlAggregateFunctionName(name: []const u8) bool {
    return std.ascii.eqlIgnoreCase(name, "count") or
        std.ascii.eqlIgnoreCase(name, "sum") or
        std.ascii.eqlIgnoreCase(name, "avg") or
        std.ascii.eqlIgnoreCase(name, "min") or
        std.ascii.eqlIgnoreCase(name, "max") or
        std.ascii.eqlIgnoreCase(name, "bool_or") or
        std.ascii.eqlIgnoreCase(name, "bool_and") or
        std.ascii.eqlIgnoreCase(name, "array_agg") or
        std.ascii.eqlIgnoreCase(name, "string_agg");
}

test "sql adapter classifier identifies write statement families" {
    const alloc = std.testing.allocator;
    const cases = [_]struct {
        sql: []const u8,
        expected: SqlWriteStatementKind,
    }{
        .{ .sql = "INSERT INTO usage_records(id) VALUES ('u1')", .expected = .insert },
        .{ .sql = "INSERT INTO usage_records(id) SELECT id FROM archived_records", .expected = .insert_source },
        .{ .sql = "WITH source_rows AS (SELECT id FROM usage_records) INSERT INTO archive(id) SELECT id FROM source_rows", .expected = .insert_source },
        .{ .sql = "WITH source_rows AS (SELECT id FROM usage_records) INSERT INTO archive(id) VALUES ('u1')", .expected = .insert },
        .{ .sql = "WITH source_rows AS MATERIALIZED (SELECT id FROM usage_records) UPDATE usage_records SET status = 'done' WHERE id IN (SELECT id FROM source_rows)", .expected = .update },
        .{ .sql = "WITH source_rows AS MATERIALIZED (SELECT id FROM usage_records) UPDATE usage_records SET status = source_rows.status FROM source_rows WHERE usage_records.id = source_rows.id", .expected = .update_joined_source },
        .{ .sql = "WITH source_rows AS NOT MATERIALIZED (SELECT id FROM usage_records) DELETE FROM usage_records WHERE id IN (SELECT id FROM source_rows)", .expected = .delete },
        .{ .sql = "WITH source_rows AS NOT MATERIALIZED (SELECT id FROM usage_records) DELETE FROM usage_records USING source_rows WHERE usage_records.id = source_rows.id", .expected = .delete_joined_source },
        .{ .sql = "WITH source_rows AS (SELECT id FROM usage_records) MERGE INTO usage_records USING source_rows ON usage_records.id = source_rows.id WHEN MATCHED THEN UPDATE SET status = 'done'", .expected = .merge },
        .{ .sql = "UPDATE usage_records SET status = 'done'", .expected = .update },
        .{ .sql = "UPDATE usage_records SET status = source_records.status FROM source_records WHERE usage_records.id = source_records.id", .expected = .update_joined_source },
        .{ .sql = "UPDATE usage_records SET status = 'archived' WHERE id IN (SELECT id FROM archived_records) RETURNING id", .expected = .update_joined_source },
        .{ .sql = "UPDATE usage_records SET status = 'archived' WHERE EXISTS (SELECT 1 FROM archived_records WHERE archived_records.id = usage_records.id)", .expected = .update_joined_source },
        .{ .sql = "UPDATE prices FOR PORTION OF valid_time FROM 3 TO 7 SET price = 99 WHERE sku = 'sku:a'", .expected = .update },
        .{ .sql = "DELETE FROM usage_records", .expected = .delete },
        .{ .sql = "DELETE FROM usage_records USING source_records WHERE usage_records.id = source_records.id", .expected = .delete_joined_source },
        .{ .sql = "DELETE FROM usage_records WHERE id IN (SELECT id FROM archived_records)", .expected = .delete_joined_source },
        .{ .sql = "DELETE FROM usage_records WHERE EXISTS (SELECT 1 FROM archived_records WHERE archived_records.id = usage_records.id)", .expected = .delete_joined_source },
        .{ .sql = "DELETE FROM usage_records WHERE status = 'expired' ORDER BY expires_at USING < LIMIT 10 RETURNING id", .expected = .delete },
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

test "sql adapter classifier identifies recursive write final statement families" {
    const alloc = std.testing.allocator;
    const cases = [_]struct {
        sql: []const u8,
        expected: SqlWriteStatementKind,
    }{
        .{ .sql = "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records) INSERT INTO archive(id) SELECT id FROM source_rows", .expected = .insert_source },
        .{ .sql = "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records UNION ALL SELECT child.id FROM usage_records AS child JOIN source_rows AS parent ON child.organization_id = parent.id) UPDATE usage_records SET status = 'done' WHERE id IN (SELECT id FROM source_rows)", .expected = .update_joined_source },
        .{ .sql = "WITH RECURSIVE source_rows AS (SELECT id, status FROM usage_records) UPDATE usage_records SET status = source_rows.status FROM source_rows WHERE usage_records.id = source_rows.id", .expected = .update_joined_source },
        .{ .sql = "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records UNION ALL SELECT child.id FROM usage_records AS child JOIN source_rows AS parent ON child.organization_id = parent.id) DELETE FROM usage_records WHERE id IN (SELECT id FROM source_rows)", .expected = .delete_joined_source },
        .{ .sql = "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records) DELETE FROM usage_records USING source_rows WHERE usage_records.id = source_rows.id", .expected = .delete_joined_source },
        .{ .sql = "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records) MERGE INTO usage_records USING source_rows ON usage_records.id = source_rows.id WHEN MATCHED THEN DELETE", .expected = .merge },
    };
    for (cases) |case| {
        var tokens = try lexer.tokenizeAlloc(alloc, case.sql);
        defer lexer.freeTokens(alloc, &tokens);
        try std.testing.expect(classifyWriteStatement(tokens.items) == null);
        try std.testing.expectEqual(case.expected, classifyRecursiveWriteStatement(tokens.items).?);
    }
}

test "sql adapter classifier identifies read statement families" {
    const alloc = std.testing.allocator;
    const cases = [_]struct {
        sql: []const u8,
        expected: SqlReadStatementKind,
    }{
        .{ .sql = "SELECT id FROM usage_records", .expected = .query },
        .{ .sql = "SELECT id FROM usage_records JOIN customers ON usage_records.customer_id = customers.id", .expected = .join },
        .{ .sql = "SELECT org.id FROM usage_records AS org LEFT JOIN LATERAL (SELECT id FROM balance_records) AS bal ON true", .expected = .lateral },
        .{ .sql = "SELECT row_number() OVER (ORDER BY amount) FROM usage_records", .expected = .window },
        .{ .sql = "SELECT customer_id, count(*) FROM usage_records GROUP BY customer_id", .expected = .aggregate },
        .{ .sql = "SELECT DISTINCT organization_id, status FROM usage_records ORDER BY organization_id", .expected = .aggregate },
        .{ .sql = "SELECT DISTINCT ON (organization_id) organization_id, status FROM usage_records ORDER BY organization_id", .expected = .query },
        .{ .sql = "SELECT id FROM usage_records UNION SELECT id FROM archived_records", .expected = .set_operation },
        .{ .sql = "WITH source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows", .expected = .query },
        .{ .sql = "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows", .expected = .recursive_cte },
    };
    for (cases) |case| {
        var tokens = try lexer.tokenizeAlloc(alloc, case.sql);
        defer lexer.freeTokens(alloc, &tokens);
        try std.testing.expectEqual(case.expected, classifyReadStatement(tokens.items).?);
    }

    var update_tokens = try lexer.tokenizeAlloc(alloc, "UPDATE usage_records SET status = 'done'");
    defer lexer.freeTokens(alloc, &update_tokens);
    try std.testing.expect(classifyReadStatement(update_tokens.items) == null);
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
        .{ .sql = "PREPARE usage_plan AS WITH RECURSIVE source_rows AS (SELECT id FROM usage_records UNION ALL SELECT child.id FROM usage_records AS child JOIN source_rows AS parent ON child.organization_id = parent.id) SELECT id FROM source_rows", .start_keyword = "with", .expected = .read },
        .{ .sql = "PREPARE usage_plan AS WITH source_rows AS (SELECT id FROM usage_records) INSERT INTO archived_records(id) SELECT id FROM source_rows", .start_keyword = "with", .expected = .write },
        .{ .sql = "PREPARE usage_plan AS WITH source_rows AS (SELECT id FROM usage_records) UPDATE usage_records SET status = 'done' WHERE id IN (SELECT id FROM source_rows)", .start_keyword = "with", .expected = .write },
        .{ .sql = "PREPARE usage_plan AS WITH RECURSIVE source_rows AS (SELECT id FROM usage_records UNION ALL SELECT child.id FROM usage_records AS child JOIN source_rows AS parent ON child.organization_id = parent.id) UPDATE usage_records SET status = 'done' WHERE id IN (SELECT id FROM source_rows)", .start_keyword = "with", .expected = .write },
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

    var malformed_recursive_tokens = try lexer.tokenizeAlloc(alloc, "PREPARE usage_plan AS WITH RECURSIVE source_rows AS (SELECT id FROM usage_records)");
    defer lexer.freeTokens(alloc, &malformed_recursive_tokens);
    const malformed_recursive_start = parser.findTopLevelKeyword(malformed_recursive_tokens.items, "with").?;
    try std.testing.expect(classifyPreparedStatementSubjectKind(malformed_recursive_tokens.items, malformed_recursive_start) == null);
}
