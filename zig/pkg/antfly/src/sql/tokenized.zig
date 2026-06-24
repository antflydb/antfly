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

const ast = @import("ast.zig");
const classifier = @import("classifier.zig");
const generated_parser = @import("generated_parser.zig");
const lexer = @import("lexer.zig");
const token_mod = @import("token.zig");

pub const Token = token_mod.Token;
const TokenKeyword = token_mod.TokenKeyword;
pub const SourceSpan = token_mod.SourceSpan;

pub const TokenRange = struct {
    start: usize,
    end: usize,
};

pub const RawSqlStatement = struct {
    family: ?classifier.SqlStatementFamily = null,
    token_start: usize = 0,
    token_end: usize = 0,
    source_span: SourceSpan = .{},

    pub fn sql(self: RawSqlStatement, source_sql: []const u8) []const u8 {
        if (self.source_span.end <= self.source_span.start or self.source_span.end > source_sql.len) return "";
        return source_sql[self.source_span.start..self.source_span.end];
    }
};

pub const ParsedReadStatement = struct {
    kind: classifier.SqlReadStatementKind,
    raw: RawSqlStatement,
};

pub const ParsedWriteStatement = struct {
    kind: classifier.SqlWriteStatementKind,
    raw: RawSqlStatement,
    recursive: bool = false,
};

pub const ParsedDdlStatement = struct {
    raw: RawSqlStatement,
};

pub const ParsedExplainStatement = struct {
    raw: RawSqlStatement,
    analyze: bool = false,
    format: ast.SqlExplainFormat = .text,
    verbose: bool = false,
    costs: bool = true,
    buffers: bool = false,
    timing: bool = true,
    summary: bool = true,
    settings: bool = false,
    wal: bool = false,
    inner_token_start: ?usize = null,
    inner_token_end: ?usize = null,
};

pub const ParsedTransactionStatement = struct {
    raw: RawSqlStatement,
};

pub const ParsedSessionStatement = struct {
    raw: RawSqlStatement,
};

pub const ParsedStatement = union(enum) {
    read: ParsedReadStatement,
    write: ParsedWriteStatement,
    ddl: ParsedDdlStatement,
    explain: ParsedExplainStatement,
    transaction: ParsedTransactionStatement,
    session: ParsedSessionStatement,
    unknown: RawSqlStatement,

    pub fn raw(self: ParsedStatement) RawSqlStatement {
        return switch (self) {
            .read => |statement| statement.raw,
            .write => |statement| statement.raw,
            .ddl => |statement| statement.raw,
            .explain => |statement| statement.raw,
            .transaction => |statement| statement.raw,
            .session => |statement| statement.raw,
            .unknown => |statement| statement,
        };
    }

    pub fn readKind(self: ParsedStatement) ?classifier.SqlReadStatementKind {
        return switch (self) {
            .read => |statement| statement.kind,
            else => null,
        };
    }

    pub fn writeKind(self: ParsedStatement) ?classifier.SqlWriteStatementKind {
        return switch (self) {
            .write => |statement| statement.kind,
            else => null,
        };
    }

    pub fn isRecursiveWrite(self: ParsedStatement) bool {
        return switch (self) {
            .write => |statement| statement.recursive,
            else => false,
        };
    }
};

pub const TokenizedSql = struct {
    sql: []const u8,
    tokens: std.ArrayListUnmanaged(Token),
    statement_family: ?classifier.SqlStatementFamily = null,
    read_statement_kind: ?classifier.SqlReadStatementKind = null,
    write_statement_kind: ?classifier.SqlWriteStatementKind = null,

    pub fn initAlloc(alloc: std.mem.Allocator, sql: []const u8) !TokenizedSql {
        var tokens = try lexer.tokenizeAlloc(alloc, sql);
        errdefer lexer.freeTokens(alloc, &tokens);
        return .{
            .sql = sql,
            .tokens = tokens,
            .statement_family = classifier.classifyStatementFamily(tokens.items),
            .read_statement_kind = classifier.classifyReadStatement(tokens.items),
            .write_statement_kind = classifier.classifyWriteStatement(tokens.items),
        };
    }

    pub fn initFromTokenSliceAlloc(alloc: std.mem.Allocator, sql: []const u8, source_tokens: []const Token) !TokenizedSql {
        var tokens = try cloneTokensAlloc(alloc, source_tokens);
        errdefer lexer.freeTokens(alloc, &tokens);
        return .{
            .sql = sql,
            .tokens = tokens,
            .statement_family = classifier.classifyStatementFamily(tokens.items),
            .read_statement_kind = classifier.classifyReadStatement(tokens.items),
            .write_statement_kind = classifier.classifyWriteStatement(tokens.items),
        };
    }

    pub fn initFromTokenRangesAlloc(
        alloc: std.mem.Allocator,
        sql: []const u8,
        source_tokens: []const Token,
        ranges: []const TokenRange,
    ) !TokenizedSql {
        var tokens = try cloneTokenRangesAlloc(alloc, source_tokens, ranges);
        errdefer lexer.freeTokens(alloc, &tokens);
        return .{
            .sql = sql,
            .tokens = tokens,
            .statement_family = classifier.classifyStatementFamily(tokens.items),
            .read_statement_kind = classifier.classifyReadStatement(tokens.items),
            .write_statement_kind = classifier.classifyWriteStatement(tokens.items),
        };
    }

    pub fn deinit(self: *TokenizedSql, alloc: std.mem.Allocator) void {
        lexer.freeTokens(alloc, &self.tokens);
        self.* = undefined;
    }

    pub fn items(self: *const TokenizedSql) []const Token {
        return self.tokens.items;
    }
};

pub const ParsedSql = struct {
    tokenized_sql: TokenizedSql,
    raw_statement: RawSqlStatement,
    statement: ParsedStatement,

    pub fn initAlloc(alloc: std.mem.Allocator, source_sql: []const u8) !ParsedSql {
        var tokenized_sql = try TokenizedSql.initAlloc(alloc, source_sql);
        errdefer tokenized_sql.deinit(alloc);
        try observeGeneratedParserGateAlloc(alloc, tokenized_sql.items());
        const raw_statement = try parseRawStatement(tokenized_sql.items(), tokenized_sql.statement_family);
        return .{
            .tokenized_sql = tokenized_sql,
            .raw_statement = raw_statement,
            .statement = parseStatement(raw_statement, &tokenized_sql),
        };
    }

    pub fn initFromTokenSliceAlloc(alloc: std.mem.Allocator, source_sql: []const u8, source_tokens: []const Token) !ParsedSql {
        var tokenized_sql = try TokenizedSql.initFromTokenSliceAlloc(alloc, source_sql, source_tokens);
        errdefer tokenized_sql.deinit(alloc);
        try observeGeneratedParserGateAlloc(alloc, tokenized_sql.items());
        const raw_statement = try parseRawStatement(tokenized_sql.items(), tokenized_sql.statement_family);
        return .{
            .tokenized_sql = tokenized_sql,
            .raw_statement = raw_statement,
            .statement = parseStatement(raw_statement, &tokenized_sql),
        };
    }

    pub fn initChildStatementAlloc(
        alloc: std.mem.Allocator,
        parent: *const ParsedSql,
        token_start: usize,
        token_end: usize,
    ) !ParsedSql {
        if (token_start >= token_end or token_end > parent.items().len) return error.UnsupportedSqlShape;
        return try initFromTokenSliceAlloc(alloc, parent.sql(), parent.items()[token_start..token_end]);
    }

    pub fn initChildStatementFromTokenRangesAlloc(
        alloc: std.mem.Allocator,
        parent: *const ParsedSql,
        ranges: []const TokenRange,
    ) !ParsedSql {
        var tokenized_sql = try TokenizedSql.initFromTokenRangesAlloc(alloc, parent.sql(), parent.items(), ranges);
        errdefer tokenized_sql.deinit(alloc);
        const raw_statement = try parseRawStatement(tokenized_sql.items(), tokenized_sql.statement_family);
        return .{
            .tokenized_sql = tokenized_sql,
            .raw_statement = raw_statement,
            .statement = parseStatement(raw_statement, &tokenized_sql),
        };
    }

    pub fn deinit(self: *ParsedSql, alloc: std.mem.Allocator) void {
        self.tokenized_sql.deinit(alloc);
        self.* = undefined;
    }

    pub fn sql(self: *const ParsedSql) []const u8 {
        return self.tokenized_sql.sql;
    }

    pub fn items(self: *const ParsedSql) []const Token {
        return self.tokenized_sql.items();
    }

    pub fn statementSql(self: *const ParsedSql) []const u8 {
        return self.raw_statement.sql(self.sql());
    }

    pub fn readStatementKind(self: *const ParsedSql) ?classifier.SqlReadStatementKind {
        return self.statement.readKind();
    }

    pub fn writeStatementKind(self: *const ParsedSql) ?classifier.SqlWriteStatementKind {
        return self.statement.writeKind();
    }

    pub fn isRecursiveWriteStatement(self: *const ParsedSql) bool {
        return self.statement.isRecursiveWrite();
    }
};

fn observeGeneratedParserGateAlloc(alloc: std.mem.Allocator, tokens: []const Token) !void {
    _ = generated_parser.parseGeneratedGateTokensAlloc(alloc, tokens) catch |err| switch (err) {
        error.UnsupportedSqlShape, error.UnexpectedToken => null,
        else => return err,
    };
}

fn parseStatement(raw_statement: RawSqlStatement, tokenized_sql: *const TokenizedSql) ParsedStatement {
    if (tokenized_sql.read_statement_kind) |kind| {
        return .{ .read = .{ .kind = kind, .raw = raw_statement } };
    }
    if (tokenized_sql.write_statement_kind) |kind| {
        return .{ .write = .{ .kind = kind, .raw = raw_statement } };
    }
    if (classifier.classifyRecursiveWriteStatement(tokenized_sql.items())) |kind| {
        return .{ .write = .{ .kind = kind, .raw = raw_statement, .recursive = true } };
    }
    return switch (tokenized_sql.statement_family orelse return .{ .unknown = raw_statement }) {
        .ddl => classifyDdlLikeStatement(raw_statement, tokenized_sql.items()),
        else => .{ .unknown = raw_statement },
    };
}

fn classifyDdlLikeStatement(raw_statement: RawSqlStatement, tokens: []const Token) ParsedStatement {
    if (tokens.len == 0 or tokens[0].kind != .identifier) return .{ .unknown = raw_statement };
    if (tokens[0].isKeyword(.explain)) return .{ .explain = parseExplainStatement(raw_statement, tokens) catch .{ .raw = raw_statement } };
    if (tokens[0].isKeyword(.begin) or tokens[0].isKeyword(.commit) or tokens[0].isKeyword(.rollback)) {
        return .{ .transaction = .{ .raw = raw_statement } };
    }
    if (tokens[0].isKeyword(.set) or tokens[0].isKeyword(.reset) or tokens[0].isKeyword(.show) or tokens[0].isKeyword(.discard)) {
        return .{ .session = .{ .raw = raw_statement } };
    }
    if (tokens[0].isKeyword(.prepare) or tokens[0].isKeyword(.execute) or tokens[0].isKeyword(.deallocate)) {
        return .{ .session = .{ .raw = raw_statement } };
    }
    return .{ .ddl = .{ .raw = raw_statement } };
}

fn parseExplainStatement(raw_statement: RawSqlStatement, tokens: []const Token) !ParsedExplainStatement {
    var index = raw_statement.token_start;
    if (!matchKeywordTag(tokens, &index, raw_statement.token_end, .explain)) return error.UnsupportedSqlShape;
    if (index >= raw_statement.token_end) return error.UnsupportedSqlShape;

    var statement = ParsedExplainStatement{ .raw = raw_statement };
    if (matchToken(tokens, &index, raw_statement.token_end, .lparen)) {
        try parseExplainOptions(tokens, &index, raw_statement.token_end, &statement);
        if (index >= raw_statement.token_end) return error.UnsupportedSqlShape;
    }

    if (matchKeywordTag(tokens, &index, raw_statement.token_end, .analyze)) {
        statement.analyze = true;
        if (index >= raw_statement.token_end) return error.UnsupportedSqlShape;
    }

    statement.inner_token_start = index;
    statement.inner_token_end = raw_statement.token_end;
    return statement;
}

fn parseExplainOptions(
    tokens: []const Token,
    index: *usize,
    end: usize,
    statement: *ParsedExplainStatement,
) !void {
    while (true) {
        if (index.* >= end) return error.UnsupportedSqlShape;
        if (matchKeywordTag(tokens, index, end, .format)) {
            if (matchKeywordTag(tokens, index, end, .json)) {
                statement.format = .json;
            } else if (matchKeywordTag(tokens, index, end, .text)) {
                statement.format = .text;
            } else {
                return error.UnsupportedSqlShape;
            }
        } else if (matchKeywordTag(tokens, index, end, .verbose)) {
            statement.verbose = parseOptionalExplainBool(tokens, index, end, true);
        } else if (matchKeywordTag(tokens, index, end, .costs)) {
            statement.costs = parseOptionalExplainBool(tokens, index, end, true);
        } else if (matchKeywordTag(tokens, index, end, .analyze)) {
            statement.analyze = parseOptionalExplainBool(tokens, index, end, true);
        } else if (matchKeywordTag(tokens, index, end, .buffers)) {
            statement.buffers = parseOptionalExplainBool(tokens, index, end, true);
        } else if (matchKeywordTag(tokens, index, end, .timing)) {
            statement.timing = parseOptionalExplainBool(tokens, index, end, true);
        } else if (matchKeywordTag(tokens, index, end, .summary)) {
            statement.summary = parseOptionalExplainBool(tokens, index, end, true);
        } else if (matchKeywordTag(tokens, index, end, .settings)) {
            statement.settings = parseOptionalExplainBool(tokens, index, end, true);
        } else if (matchKeywordTag(tokens, index, end, .wal)) {
            statement.wal = parseOptionalExplainBool(tokens, index, end, true);
        } else {
            return error.UnsupportedSqlShape;
        }

        if (matchToken(tokens, index, end, .comma)) continue;
        if (matchToken(tokens, index, end, .rparen)) return;
        return error.UnsupportedSqlShape;
    }
}

fn parseOptionalExplainBool(tokens: []const Token, index: *usize, end: usize, default_value: bool) bool {
    const before = index.*;
    if (matchKeywordTag(tokens, index, end, .true) or
        matchKeywordTag(tokens, index, end, .on) or
        matchKeywordTag(tokens, index, end, .yes))
    {
        return true;
    }
    index.* = before;
    if (matchKeywordTag(tokens, index, end, .false) or
        matchKeywordTag(tokens, index, end, .off) or
        matchKeywordTag(tokens, index, end, .no))
    {
        return false;
    }
    index.* = before;
    return default_value;
}

fn matchKeywordTag(tokens: []const Token, index: *usize, end: usize, keyword: TokenKeyword) bool {
    if (index.* >= end or index.* >= tokens.len) return false;
    if (!tokens[index.*].matchesKeywordTag(keyword)) return false;
    index.* += 1;
    return true;
}

fn matchToken(tokens: []const Token, index: *usize, end: usize, kind: token_mod.TokenKind) bool {
    if (index.* >= end or index.* >= tokens.len or tokens[index.*].kind != kind) return false;
    index.* += 1;
    return true;
}

fn cloneTokensAlloc(alloc: std.mem.Allocator, source_tokens: []const Token) !std.ArrayListUnmanaged(Token) {
    var out = try std.ArrayListUnmanaged(Token).initCapacity(alloc, source_tokens.len);
    errdefer lexer.freeTokens(alloc, &out);
    for (source_tokens) |token| {
        var cloned = token;
        if (token.owned) {
            cloned.text = try alloc.dupe(u8, token.text);
            cloned.owned = true;
        } else {
            cloned.owned = false;
        }
        out.appendAssumeCapacity(cloned);
    }
    return out;
}

fn cloneTokenRangesAlloc(
    alloc: std.mem.Allocator,
    source_tokens: []const Token,
    ranges: []const TokenRange,
) !std.ArrayListUnmanaged(Token) {
    var total: usize = 0;
    for (ranges) |range| {
        if (range.start >= range.end or range.end > source_tokens.len) return error.UnsupportedSqlShape;
        total += range.end - range.start;
    }
    var out = try std.ArrayListUnmanaged(Token).initCapacity(alloc, total);
    errdefer lexer.freeTokens(alloc, &out);
    for (ranges) |range| {
        for (source_tokens[range.start..range.end]) |token| {
            var cloned = token;
            if (token.owned) {
                cloned.text = try alloc.dupe(u8, token.text);
                cloned.owned = true;
            } else {
                cloned.owned = false;
            }
            out.appendAssumeCapacity(cloned);
        }
    }
    return out;
}

fn parseRawStatement(tokens: []const Token, family: ?classifier.SqlStatementFamily) !RawSqlStatement {
    if (tokens.len == 0) return .{ .family = family };
    var token_end = try rawStatementTokenEnd(tokens);
    while (token_end > 0 and tokens[token_end - 1].kind == .semicolon) token_end -= 1;
    if (token_end == 0) return .{ .family = family };
    return .{
        .family = family,
        .token_start = 0,
        .token_end = token_end,
        .source_span = .{
            .start = tokens[0].source_start,
            .end = tokens[token_end - 1].source_end,
        },
    };
}

fn rawStatementTokenEnd(tokens: []const Token) !usize {
    var depth: usize = 0;
    for (tokens, 0..) |token, i| {
        switch (token.kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth > 0) depth -= 1;
            },
            .semicolon => if (depth == 0) {
                var next = i + 1;
                while (next < tokens.len and tokens[next].kind == .semicolon) next += 1;
                if (next < tokens.len) return error.UnsupportedSqlShape;
                return i;
            },
            else => {},
        }
    }
    return tokens.len;
}

test "sql adapter tokenized sql classifies read and write statements once" {
    const alloc = std.testing.allocator;

    var query = try TokenizedSql.initAlloc(alloc, "SELECT id FROM usage_records WHERE status = 'open'");
    defer query.deinit(alloc);
    try std.testing.expectEqual(classifier.SqlStatementFamily.select, query.statement_family.?);
    try std.testing.expectEqual(classifier.SqlReadStatementKind.query, query.read_statement_kind.?);
    try std.testing.expect(query.write_statement_kind == null);

    var joined = try TokenizedSql.initAlloc(alloc, "SELECT o.id FROM usage_records AS o JOIN customers AS c ON o.customer_id = c.id");
    defer joined.deinit(alloc);
    try std.testing.expectEqual(classifier.SqlReadStatementKind.join, joined.read_statement_kind.?);

    var distinct_on = try ParsedSql.initAlloc(alloc, "SELECT DISTINCT ON (organization_id) organization_id, id FROM usage_records ORDER BY organization_id ASC, created_at DESC");
    defer distinct_on.deinit(alloc);
    try std.testing.expectEqual(classifier.SqlReadStatementKind.query, distinct_on.readStatementKind().?);

    var write = try TokenizedSql.initAlloc(alloc, "WITH source_rows AS (SELECT id FROM usage_records) UPDATE usage_records SET status = 'done' WHERE id IN (SELECT id FROM source_rows)");
    defer write.deinit(alloc);
    try std.testing.expectEqual(classifier.SqlStatementFamily.with, write.statement_family.?);
    try std.testing.expect(write.read_statement_kind == null);
    try std.testing.expectEqual(classifier.SqlWriteStatementKind.update, write.write_statement_kind.?);
}

test "sql adapter parsed sql exposes raw statement source spans" {
    const alloc = std.testing.allocator;
    const sql = "  SELECT id FROM usage_records;  ";

    var parsed = try ParsedSql.initAlloc(alloc, sql);
    defer parsed.deinit(alloc);

    try std.testing.expectEqual(classifier.SqlStatementFamily.select, parsed.raw_statement.family.?);
    try std.testing.expectEqualStrings("SELECT id FROM usage_records", parsed.statementSql());
    try std.testing.expectEqual(@as(usize, 2), parsed.raw_statement.source_span.start);
    try std.testing.expectEqual(@as(usize, 30), parsed.raw_statement.source_span.end);

    var trailing_semicolons = try ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records;;");
    defer trailing_semicolons.deinit(alloc);
    try std.testing.expectEqualStrings("SELECT id FROM usage_records", trailing_semicolons.statementSql());

    try std.testing.expectError(
        error.UnsupportedSqlShape,
        ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records; DROP TABLE usage_records"),
    );

    var nested_semicolon = try ParsedSql.initAlloc(alloc, "SELECT ';' AS separator");
    defer nested_semicolon.deinit(alloc);
    try std.testing.expectEqualStrings("SELECT ';' AS separator", nested_semicolon.statementSql());
}

test "sql adapter parsed sql does not require generated grammar parity" {
    const alloc = std.testing.allocator;

    var ddl = try ParsedSql.initAlloc(alloc, "ALTER TABLE audit_log ALTER COLUMN amount TYPE numeric USING amount + 1;");
    defer ddl.deinit(alloc);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .ddl), std.meta.activeTag(ddl.statement));

    var select = try ParsedSql.initAlloc(alloc, "SELECT id FROM docs WHERE status = 'active' LIMIT 5");
    defer select.deinit(alloc);
    try std.testing.expectEqual(classifier.SqlReadStatementKind.query, select.readStatementKind().?);
}

test "sql adapter parsed sql builds non-contiguous child statements from parent tokens" {
    const alloc = std.testing.allocator;

    var parent = try ParsedSql.initAlloc(
        alloc,
        "SELECT account_id, total INTO usage_archive FROM usage_records WHERE total > 10",
    );
    defer parent.deinit(alloc);

    const ranges = [_]TokenRange{
        .{ .start = 0, .end = 4 },
        .{ .start = 6, .end = parent.items().len },
    };
    var child = try ParsedSql.initChildStatementFromTokenRangesAlloc(alloc, &parent, &ranges);
    defer child.deinit(alloc);

    try std.testing.expectEqual(classifier.SqlStatementFamily.select, child.raw_statement.family.?);
    try std.testing.expectEqual(classifier.SqlReadStatementKind.query, child.readStatementKind().?);
    try std.testing.expectEqual(@as(usize, parent.items().len - 2), child.items().len);
    try std.testing.expectEqualStrings("SELECT", child.items()[0].text);
    try std.testing.expectEqualStrings("FROM", child.items()[4].text);
    try std.testing.expectEqualStrings("usage_records", child.items()[5].text);
}

test "sql adapter parsed sql owns typed statement variants" {
    const alloc = std.testing.allocator;

    var read = try ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records");
    defer read.deinit(alloc);
    switch (read.statement) {
        .read => |statement| {
            try std.testing.expectEqual(classifier.SqlReadStatementKind.query, statement.kind);
            try std.testing.expectEqualStrings("SELECT id FROM usage_records", statement.raw.sql(read.sql()));
        },
        else => return error.TestUnexpectedResult,
    }

    var write = try ParsedSql.initAlloc(alloc, "UPDATE usage_records SET status = 'done' WHERE id = 'u1'");
    defer write.deinit(alloc);
    switch (write.statement) {
        .write => |statement| {
            try std.testing.expectEqual(classifier.SqlWriteStatementKind.update, statement.kind);
            try std.testing.expect(!statement.recursive);
            try std.testing.expect(!write.isRecursiveWriteStatement());
        },
        else => return error.TestUnexpectedResult,
    }

    var recursive_write = try ParsedSql.initAlloc(alloc, "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records) INSERT INTO archive(id) SELECT id FROM source_rows");
    defer recursive_write.deinit(alloc);
    switch (recursive_write.statement) {
        .write => |statement| {
            try std.testing.expectEqual(classifier.SqlWriteStatementKind.insert_source, statement.kind);
            try std.testing.expect(statement.recursive);
            try std.testing.expect(recursive_write.isRecursiveWriteStatement());
        },
        else => return error.TestUnexpectedResult,
    }

    var explain = try ParsedSql.initAlloc(alloc, "EXPLAIN SELECT id FROM usage_records;");
    defer explain.deinit(alloc);
    switch (explain.statement) {
        .explain => |statement| {
            try std.testing.expectEqualStrings("EXPLAIN SELECT id FROM usage_records;", statement.raw.sql(explain.sql()));
            try std.testing.expect(!statement.analyze);
            try std.testing.expectEqual(ast.SqlExplainFormat.text, statement.format);
            try std.testing.expect(!statement.verbose);
            try std.testing.expect(statement.costs);
            try std.testing.expectEqual(@as(?usize, 1), statement.inner_token_start);
            try std.testing.expectEqual(@as(?usize, 5), statement.inner_token_end);
        },
        else => return error.TestUnexpectedResult,
    }

    var explain_options = try ParsedSql.initAlloc(alloc, "EXPLAIN (FORMAT JSON, VERBOSE, COSTS OFF, ANALYZE ON, BUFFERS, TIMING OFF, SUMMARY OFF, SETTINGS ON, WAL) SELECT id FROM usage_records");
    defer explain_options.deinit(alloc);
    switch (explain_options.statement) {
        .explain => |statement| {
            try std.testing.expect(statement.analyze);
            try std.testing.expectEqual(ast.SqlExplainFormat.json, statement.format);
            try std.testing.expect(statement.verbose);
            try std.testing.expect(!statement.costs);
            try std.testing.expect(statement.buffers);
            try std.testing.expect(!statement.timing);
            try std.testing.expect(!statement.summary);
            try std.testing.expect(statement.settings);
            try std.testing.expect(statement.wal);
            try std.testing.expect(statement.inner_token_start != null);
            try std.testing.expect(statement.inner_token_end != null);
        },
        else => return error.TestUnexpectedResult,
    }

    var explain_analyze = try ParsedSql.initAlloc(alloc, "EXPLAIN ANALYZE INSERT INTO usage_records (id) VALUES ('u1')");
    defer explain_analyze.deinit(alloc);
    switch (explain_analyze.statement) {
        .explain => |statement| {
            try std.testing.expect(statement.analyze);
            try std.testing.expectEqualStrings("INSERT", explain_analyze.items()[statement.inner_token_start.?].text);
        },
        else => return error.TestUnexpectedResult,
    }

    var invalid_explain = try ParsedSql.initAlloc(alloc, "EXPLAIN (FORMAT YAML) SELECT 1");
    defer invalid_explain.deinit(alloc);
    switch (invalid_explain.statement) {
        .explain => |statement| {
            try std.testing.expect(statement.inner_token_start == null);
            try std.testing.expect(statement.inner_token_end == null);
        },
        else => return error.TestUnexpectedResult,
    }

    var empty_explain = try ParsedSql.initAlloc(alloc, "EXPLAIN");
    defer empty_explain.deinit(alloc);
    switch (empty_explain.statement) {
        .explain => |statement| {
            try std.testing.expect(statement.inner_token_start == null);
            try std.testing.expect(statement.inner_token_end == null);
        },
        else => return error.TestUnexpectedResult,
    }

    var session = try ParsedSql.initAlloc(alloc, "SET search_path TO public");
    defer session.deinit(alloc);
    switch (session.statement) {
        .session => {},
        else => return error.TestUnexpectedResult,
    }

    var ddl = try ParsedSql.initAlloc(alloc, "CREATE TABLE usage_records (id text)");
    defer ddl.deinit(alloc);
    switch (ddl.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }
}
