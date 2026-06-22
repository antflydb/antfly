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

const classifier = @import("classifier.zig");
const lexer = @import("lexer.zig");
const token_mod = @import("token.zig");

pub const Token = token_mod.Token;
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
};

pub const ParsedDdlStatement = struct {
    raw: RawSqlStatement,
};

pub const ParsedExplainStatement = struct {
    raw: RawSqlStatement,
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
};

fn parseStatement(raw_statement: RawSqlStatement, tokenized_sql: *const TokenizedSql) ParsedStatement {
    if (tokenized_sql.read_statement_kind) |kind| {
        return .{ .read = .{ .kind = kind, .raw = raw_statement } };
    }
    if (tokenized_sql.write_statement_kind) |kind| {
        return .{ .write = .{ .kind = kind, .raw = raw_statement } };
    }
    return switch (tokenized_sql.statement_family orelse return .{ .unknown = raw_statement }) {
        .ddl => classifyDdlLikeStatement(raw_statement, tokenized_sql.items()),
        else => .{ .unknown = raw_statement },
    };
}

fn classifyDdlLikeStatement(raw_statement: RawSqlStatement, tokens: []const Token) ParsedStatement {
    if (tokens.len == 0 or tokens[0].kind != .identifier) return .{ .unknown = raw_statement };
    if (tokens[0].isKeyword(.explain)) return .{ .explain = .{ .raw = raw_statement } };
    if (tokens[0].isKeyword(.begin)) return .{ .transaction = .{ .raw = raw_statement } };
    if (tokens[0].isKeyword(.set)) return .{ .session = .{ .raw = raw_statement } };
    return .{ .ddl = .{ .raw = raw_statement } };
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
        .write => |statement| try std.testing.expectEqual(classifier.SqlWriteStatementKind.update, statement.kind),
        else => return error.TestUnexpectedResult,
    }

    var explain = try ParsedSql.initAlloc(alloc, "EXPLAIN SELECT id FROM usage_records");
    defer explain.deinit(alloc);
    switch (explain.statement) {
        .explain => |statement| try std.testing.expectEqualStrings("EXPLAIN SELECT id FROM usage_records", statement.raw.sql(explain.sql())),
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
