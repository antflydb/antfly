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

const generated = @import("grammar/generated/root.zig");
const lexer = @import("lexer.zig");
const token_mod = @import("token.zig");

pub const GeneratedSqlStatementKind = enum {
    session,
    transaction,
    prepared,
    ddl,
    dml,
    read,
    other,
};

pub const GeneratedSqlSessionKind = enum {
    set,
    reset,
    show,
    discard_all,
};

pub const GeneratedSqlTransactionKind = enum {
    begin,
    commit,
    rollback,
};

pub const GeneratedSqlPreparedKind = enum {
    prepare,
    execute,
    deallocate,
};

pub const GeneratedSqlDdlKind = enum {
    create_database,
    create_schema,
    create_table,
    create_index,
    create_extension,
    alter_table,
    drop_table,
    drop_index,
    drop_schema,
    drop_database,
    drop_extension,
    create_graph_index,
    create_graph_metric,
};

pub const GeneratedSqlDmlKind = enum {
    insert_values,
    insert_select,
    update,
    delete,
    truncate,
    merge,
};

pub const GeneratedSqlReadKind = enum {
    query,
    aggregate,
    join,
    lateral,
    cte,
};

pub const GeneratedSqlStatement = union(GeneratedSqlStatementKind) {
    session: GeneratedSqlSessionKind,
    transaction: GeneratedSqlTransactionKind,
    prepared: GeneratedSqlPreparedKind,
    ddl: GeneratedSqlDdlKind,
    dml: GeneratedSqlDmlKind,
    read: GeneratedSqlReadKind,
    other: void,
};

pub const GeneratedSqlParseResult = struct {
    kind: GeneratedSqlStatementKind,
    statement: GeneratedSqlStatement,
};

pub const GeneratedSqlDiagnostic = struct {
    state: u16,
    lookahead: u16,
    token_index: usize,
    source_start: usize,
    source_end: usize,
    expected: []const []const u8,
    actual: []const u8,
};

const DiagnosticSpan = struct {
    start: usize,
    end: usize,
    actual: []const u8,
};

pub const GeneratedSqlCorpusCase = struct {
    sql: []const u8,
    kind: GeneratedSqlStatementKind,
};

pub const first_family_corpus = [_]GeneratedSqlCorpusCase{
    .{ .sql = "SET antfly.sync_level = 'write'", .kind = .session },
    .{ .sql = "SET search_path public", .kind = .session },
    .{ .sql = "SET search_path TO public", .kind = .session },
    .{ .sql = "RESET search_path", .kind = .session },
    .{ .sql = "SHOW search_path", .kind = .session },
    .{ .sql = "DISCARD ALL", .kind = .session },
    .{ .sql = "BEGIN", .kind = .transaction },
    .{ .sql = "COMMIT", .kind = .transaction },
    .{ .sql = "ROLLBACK", .kind = .transaction },
    .{ .sql = "PREPARE read_stmt AS SELECT id FROM usage_records", .kind = .prepared },
    .{ .sql = "EXECUTE read_stmt()", .kind = .prepared },
    .{ .sql = "DEALLOCATE read_stmt", .kind = .prepared },
};

pub const simple_ddl_corpus = [_]GeneratedSqlCorpusCase{
    .{ .sql = "CREATE DATABASE tenant_ops", .kind = .ddl },
    .{ .sql = "CREATE SCHEMA analytics", .kind = .ddl },
    .{ .sql = "CREATE SCHEMA IF NOT EXISTS analytics", .kind = .ddl },
    .{ .sql = "CREATE TABLE usage_records (id text PRIMARY KEY, status text DEFAULT 'open')", .kind = .ddl },
    .{ .sql = "CREATE TABLE IF NOT EXISTS usage_records (id text PRIMARY KEY)", .kind = .ddl },
    .{ .sql = "CREATE INDEX usage_records_status_idx ON usage_records (status)", .kind = .ddl },
    .{ .sql = "CREATE INDEX IF NOT EXISTS usage_records_status_idx ON usage_records (status)", .kind = .ddl },
    .{ .sql = "CREATE EXTENSION vector", .kind = .ddl },
    .{ .sql = "DROP TABLE usage_records", .kind = .ddl },
    .{ .sql = "DROP TABLE IF EXISTS usage_records", .kind = .ddl },
    .{ .sql = "DROP INDEX usage_records_status_idx", .kind = .ddl },
    .{ .sql = "DROP SCHEMA analytics CASCADE", .kind = .ddl },
    .{ .sql = "DROP DATABASE tenant_ops", .kind = .ddl },
};

pub const simple_dml_corpus = [_]GeneratedSqlCorpusCase{
    .{ .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'open')", .kind = .dml },
    .{ .sql = "INSERT INTO usage_records (id) SELECT id FROM incoming_usage", .kind = .dml },
    .{ .sql = "UPDATE usage_records SET status = 'done' WHERE id = 'u1' RETURNING id", .kind = .dml },
    .{ .sql = "DELETE FROM usage_records WHERE id = 'u1' RETURNING id", .kind = .dml },
    .{ .sql = "TRUNCATE usage_records", .kind = .dml },
    .{ .sql = "MERGE INTO usage_records USING source_rows ON usage_records.id = source_rows.id WHEN MATCHED THEN UPDATE SET status = source_rows.status", .kind = .dml },
};

pub const simple_read_corpus = [_]GeneratedSqlCorpusCase{
    .{ .sql = "SELECT id, status FROM usage_records WHERE status = 'open' ORDER BY id LIMIT 10", .kind = .read },
    .{ .sql = "SELECT status FROM usage_records GROUP BY status HAVING status = 'open'", .kind = .read },
    .{ .sql = "SELECT usage_records.id FROM usage_records JOIN accounts ON usage_records.account_id = accounts.id", .kind = .read },
    .{ .sql = "SELECT id FROM LATERAL (SELECT id FROM usage_records) AS source_rows", .kind = .read },
    .{ .sql = "WITH source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows", .kind = .read },
};

pub fn parseSqlAlloc(alloc: std.mem.Allocator, sql: []const u8) !GeneratedSqlParseResult {
    var tokens = try lexer.tokenizeAlloc(alloc, sql);
    defer lexer.freeTokens(alloc, &tokens);
    return try parseTokensAlloc(alloc, tokens.items);
}

pub fn parseTokensAlloc(alloc: std.mem.Allocator, tokens: []const token_mod.Token) !GeneratedSqlParseResult {
    const token_ids = try tokenIdsAlloc(alloc, tokens);
    defer alloc.free(token_ids);
    try generated.parse(alloc, token_ids);
    const statement = classifyStatement(tokens);
    return .{ .kind = std.meta.activeTag(statement), .statement = statement };
}

pub fn parseFirstFamilyTokensAlloc(alloc: std.mem.Allocator, tokens: []const token_mod.Token) !?GeneratedSqlParseResult {
    if (!isFirstFamilyTokens(tokens)) return null;
    return try parseTokensAlloc(alloc, tokens);
}

pub fn parseGeneratedGateTokensAlloc(alloc: std.mem.Allocator, tokens: []const token_mod.Token) !?GeneratedSqlParseResult {
    const kind = classifyTokens(tokens);
    if (kind == .other) return null;
    return parseTokensAlloc(alloc, tokens) catch |err| switch (err) {
        error.UnsupportedSqlShape, error.UnexpectedToken => if (kind == .ddl or kind == .dml or kind == .read) null else err,
        else => err,
    };
}

pub fn isFirstFamilyTokens(tokens: []const token_mod.Token) bool {
    const kind = classifyTokens(tokens);
    return kind == .session or kind == .transaction or kind == .prepared;
}

pub fn isGeneratedGateTokens(tokens: []const token_mod.Token) bool {
    return classifyTokens(tokens) != .other;
}

pub fn diagnosticAlloc(alloc: std.mem.Allocator, tokens: []const token_mod.Token) !?GeneratedSqlDiagnostic {
    const token_ids = try tokenIdsAlloc(alloc, tokens);
    defer alloc.free(token_ids);
    const info = try generated.parseError(alloc, token_ids) orelse return null;
    const actions = generated.actionsForState(info.state);
    const expected = try alloc.alloc([]const u8, actions.len);
    for (actions, 0..) |action, idx| expected[idx] = generated.symbolName(action.terminal);
    const span: DiagnosticSpan = if (info.token_index < tokens.len)
        .{ .start = tokens[info.token_index].source_start, .end = tokens[info.token_index].source_end, .actual = tokens[info.token_index].text }
    else
        .{ .start = if (tokens.len == 0) 0 else tokens[tokens.len - 1].source_end, .end = if (tokens.len == 0) 0 else tokens[tokens.len - 1].source_end, .actual = "$end" };
    return .{
        .state = info.state,
        .lookahead = info.lookahead,
        .token_index = info.token_index,
        .source_start = span.start,
        .source_end = span.end,
        .expected = expected,
        .actual = span.actual,
    };
}

pub fn tokenIdsAlloc(alloc: std.mem.Allocator, tokens: []const token_mod.Token) ![]u16 {
    var ids: std.ArrayListUnmanaged(u16) = .empty;
    errdefer ids.deinit(alloc);
    for (tokens, 0..) |tok, index| {
        if (tok.kind == .semicolon and trailingSemicolonOnly(tokens, index)) break;
        try appendTokenIds(alloc, &ids, tok);
    }
    return try ids.toOwnedSlice(alloc);
}

fn appendTokenIds(alloc: std.mem.Allocator, ids: *std.ArrayListUnmanaged(u16), tok: token_mod.Token) !void {
    switch (tok.kind) {
        .identifier => {
            if (try keywordSymbolIdAlloc(alloc, tok)) |id| {
                try ids.append(alloc, id);
                return;
            }
            try appendIdentifierIds(alloc, ids, tok.text);
        },
        .string => try appendSymbol(ids, alloc, "STRING"),
        .number => try appendSymbol(ids, alloc, "NUMBER"),
        .placeholder => try appendSymbol(ids, alloc, "PLACEHOLDER"),
        .comma => try appendSymbol(ids, alloc, "COMMA"),
        .star => try appendSymbol(ids, alloc, "STAR"),
        .eq => try appendSymbol(ids, alloc, "EQ"),
        .neq => try appendSymbol(ids, alloc, "NEQ"),
        .gt => try appendSymbol(ids, alloc, "GT"),
        .gte => try appendSymbol(ids, alloc, "GTE"),
        .lt => try appendSymbol(ids, alloc, "LT"),
        .lte => try appendSymbol(ids, alloc, "LTE"),
        .plus => try appendSymbol(ids, alloc, "PLUS"),
        .minus => try appendSymbol(ids, alloc, "MINUS"),
        .slash => try appendSymbol(ids, alloc, "SLASH"),
        .percent => try appendSymbol(ids, alloc, "PERCENT"),
        .lparen => try appendSymbol(ids, alloc, "LPAREN"),
        .rparen => try appendSymbol(ids, alloc, "RPAREN"),
        .lbracket => try appendSymbol(ids, alloc, "LBRACKET"),
        .rbracket => try appendSymbol(ids, alloc, "RBRACKET"),
        .arrow_json => try appendSymbol(ids, alloc, "ARROW_JSON"),
        .arrow_text => try appendSymbol(ids, alloc, "ARROW_TEXT"),
        .path_arrow_json => try appendSymbol(ids, alloc, "PATH_ARROW_JSON"),
        .path_arrow_text => try appendSymbol(ids, alloc, "PATH_ARROW_TEXT"),
        .semicolon => try appendSymbol(ids, alloc, "SEMICOLON"),
        else => return error.UnsupportedSqlShape,
    }
}

fn appendIdentifierIds(alloc: std.mem.Allocator, ids: *std.ArrayListUnmanaged(u16), text: []const u8) !void {
    var parts = std.mem.splitScalar(u8, text, '.');
    var emitted = false;
    while (parts.next()) |part| {
        if (part.len == 0) return error.UnsupportedSqlShape;
        if (emitted) try appendSymbol(ids, alloc, "DOT");
        try appendSymbol(ids, alloc, "IDENT");
        emitted = true;
    }
}

fn appendSymbol(ids: *std.ArrayListUnmanaged(u16), alloc: std.mem.Allocator, name: []const u8) !void {
    const id = generated.symbolId(name) orelse return error.UnsupportedSqlShape;
    try ids.append(alloc, id);
}

fn keywordSymbolIdAlloc(alloc: std.mem.Allocator, tok: token_mod.Token) !?u16 {
    if (tok.keyword == null) return null;
    const name = try uppercaseKeywordAlloc(alloc, tok.text);
    defer alloc.free(name);
    return generated.symbolId(name);
}

fn uppercaseKeywordAlloc(alloc: std.mem.Allocator, text: []const u8) ![]u8 {
    var out = try alloc.alloc(u8, text.len);
    for (text, 0..) |ch, idx| {
        out[idx] = switch (ch) {
            'a'...'z' => ch - 'a' + 'A',
            else => ch,
        };
    }
    return out;
}

fn trailingSemicolonOnly(tokens: []const token_mod.Token, index: usize) bool {
    if (tokens[index].kind != .semicolon) return false;
    for (tokens[index + 1 ..]) |tok| {
        if (tok.kind != .semicolon) return false;
    }
    return true;
}

fn classifyTokens(tokens: []const token_mod.Token) GeneratedSqlStatementKind {
    return std.meta.activeTag(classifyStatement(tokens));
}

fn classifyStatement(tokens: []const token_mod.Token) GeneratedSqlStatement {
    if (tokens.len == 0) return .other;
    const first = tokens[0];
    if (first.matchesKeywordTag(.set)) return .{ .session = .set };
    if (first.matchesKeywordTag(.reset)) return .{ .session = .reset };
    if (first.matchesKeywordTag(.show)) return .{ .session = .show };
    if (first.matchesKeywordTag(.discard)) return .{ .session = .discard_all };
    if (first.matchesKeywordTag(.begin)) return .{ .transaction = .begin };
    if (first.matchesKeywordTag(.commit)) return .{ .transaction = .commit };
    if (first.matchesKeywordTag(.rollback)) return .{ .transaction = .rollback };
    if (first.matchesKeywordTag(.prepare)) return .{ .prepared = .prepare };
    if (first.matchesKeywordTag(.execute)) return .{ .prepared = .execute };
    if (first.matchesKeywordTag(.deallocate)) return .{ .prepared = .deallocate };
    if (first.matchesKeywordTag(.create) and tokens.len > 1) {
        const second = tokens[1];
        if (second.matchesKeywordTag(.database)) return .{ .ddl = .create_database };
        if (second.matchesKeywordTag(.schema)) return .{ .ddl = .create_schema };
        if (second.matchesKeywordTag(.table)) return .{ .ddl = .create_table };
        if (second.matchesKeywordTag(.index)) return .{ .ddl = .create_index };
        if (second.matchesKeywordTag(.extension)) return .{ .ddl = .create_extension };
        if (second.matchesKeywordTag(.graph) and tokens.len > 2) {
            if (tokens[2].matchesKeywordTag(.index)) return .{ .ddl = .create_graph_index };
            if (tokens[2].matchesKeywordTag(.metric)) return .{ .ddl = .create_graph_metric };
        }
    }
    if (first.matchesKeywordTag(.alter) and tokens.len > 1 and tokens[1].matchesKeywordTag(.table)) {
        return .{ .ddl = .alter_table };
    }
    if (first.matchesKeywordTag(.drop) and tokens.len > 1) {
        const second = tokens[1];
        if (second.matchesKeywordTag(.table)) return .{ .ddl = .drop_table };
        if (second.matchesKeywordTag(.index)) return .{ .ddl = .drop_index };
        if (second.matchesKeywordTag(.schema)) return .{ .ddl = .drop_schema };
        if (second.matchesKeywordTag(.database)) return .{ .ddl = .drop_database };
        if (second.matchesKeywordTag(.extension)) return .{ .ddl = .drop_extension };
    }
    if (first.matchesKeywordTag(.insert)) {
        for (tokens) |token| {
            if (token.matchesKeywordTag(.select)) return .{ .dml = .insert_select };
        }
        return .{ .dml = .insert_values };
    }
    if (first.matchesKeywordTag(.update)) return .{ .dml = .update };
    if (first.matchesKeywordTag(.delete)) return .{ .dml = .delete };
    if (first.matchesKeywordTag(.truncate)) return .{ .dml = .truncate };
    if (first.matchesKeywordTag(.merge)) return .{ .dml = .merge };
    if (first.matchesKeywordTag(.select) or first.matchesKeywordTag(.with)) {
        return .{ .read = classifyReadKind(tokens) };
    }
    return .other;
}

fn classifyReadKind(tokens: []const token_mod.Token) GeneratedSqlReadKind {
    if (tokens.len > 0 and tokens[0].matchesKeywordTag(.with)) return .cte;
    for (tokens) |token| {
        if (token.matchesKeywordTag(.lateral)) return .lateral;
    }
    for (tokens) |token| {
        if (token.matchesKeywordTag(.join)) return .join;
    }
    for (tokens) |token| {
        if (token.matchesKeywordTag(.group) or token.matchesKeywordTag(.having)) return .aggregate;
    }
    return .query;
}

test "generated SQL parser accepts session and control statements" {
    for (first_family_corpus) |case| {
        const result = try parseSqlAlloc(std.testing.allocator, case.sql);
        try std.testing.expectEqual(case.kind, result.kind);
    }
}

test "generated SQL parser facade classifies gated corpus" {
    const corpus = first_family_corpus ++ simple_ddl_corpus ++ simple_dml_corpus ++ simple_read_corpus;
    for (corpus) |case| {
        const generated_result = try parseSqlAlloc(std.testing.allocator, case.sql);
        try std.testing.expectEqual(case.kind, generated_result.kind);
    }
}

test "generated SQL parser facade exposes typed statement nodes" {
    try std.testing.expectEqual(GeneratedSqlStatement{ .session = .set }, (try parseSqlAlloc(std.testing.allocator, "SET search_path TO public")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .transaction = .rollback }, (try parseSqlAlloc(std.testing.allocator, "ROLLBACK")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .prepared = .execute }, (try parseSqlAlloc(std.testing.allocator, "EXECUTE read_stmt()")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .create_table }, (try parseSqlAlloc(std.testing.allocator, "CREATE TABLE usage_records (id text)")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .drop_schema }, (try parseSqlAlloc(std.testing.allocator, "DROP SCHEMA analytics CASCADE")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .insert_values }, (try parseSqlAlloc(std.testing.allocator, "INSERT INTO usage_records (id) VALUES ('u1')")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .update }, (try parseSqlAlloc(std.testing.allocator, "UPDATE usage_records SET status = 'done' WHERE id = 'u1'")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .read = .query }, (try parseSqlAlloc(std.testing.allocator, "SELECT id FROM usage_records")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .read = .cte }, (try parseSqlAlloc(std.testing.allocator, "WITH source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows")).statement);
}

test "generated SQL parser reports source-aware diagnostics" {
    var tokens = try lexer.tokenizeAlloc(std.testing.allocator, "CREATE TABLE usage_records (id)");
    defer lexer.freeTokens(std.testing.allocator, &tokens);
    const diagnostic = try diagnosticAlloc(std.testing.allocator, tokens.items) orelse return error.ExpectedDiagnostic;
    defer std.testing.allocator.free(diagnostic.expected);
    try std.testing.expect(diagnostic.expected.len > 0);
    try std.testing.expectEqualStrings(")", diagnostic.actual);
    try std.testing.expect(diagnostic.source_end >= diagnostic.source_start);
}

test "generated SQL parser rejects unsupported token shapes" {
    try std.testing.expectError(error.UnsupportedSqlShape, parseSqlAlloc(std.testing.allocator, "SELECT a @> b"));
}
