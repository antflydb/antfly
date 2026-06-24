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

pub const ParsedPreparedStatement = struct {
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

pub const GeneratedRawSqlStatement = struct {
    raw: RawSqlStatement,
    statement: generated_parser.GeneratedSqlStatement,
    ast: ?generated_parser.GeneratedSqlAst = null,

    pub fn kind(self: GeneratedRawSqlStatement) generated_parser.GeneratedSqlStatementKind {
        return std.meta.activeTag(self.statement);
    }

    pub fn deinit(self: *GeneratedRawSqlStatement, alloc: std.mem.Allocator) void {
        if (self.ast) |*generated_ast| generated_ast.deinit(alloc);
        self.* = undefined;
    }
};

pub const ParsedStatement = union(enum) {
    read: ParsedReadStatement,
    write: ParsedWriteStatement,
    ddl: ParsedDdlStatement,
    explain: ParsedExplainStatement,
    transaction: ParsedTransactionStatement,
    prepared: ParsedPreparedStatement,
    session: ParsedSessionStatement,
    unknown: RawSqlStatement,

    pub fn raw(self: ParsedStatement) RawSqlStatement {
        return switch (self) {
            .read => |statement| statement.raw,
            .write => |statement| statement.raw,
            .ddl => |statement| statement.raw,
            .explain => |statement| statement.raw,
            .transaction => |statement| statement.raw,
            .prepared => |statement| statement.raw,
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
    generated_statement: ?GeneratedRawSqlStatement = null,
    statement: ParsedStatement,

    pub fn initAlloc(alloc: std.mem.Allocator, source_sql: []const u8) !ParsedSql {
        var tokenized_sql = try TokenizedSql.initAlloc(alloc, source_sql);
        errdefer tokenized_sql.deinit(alloc);
        const raw_statement = try parseRawStatement(tokenized_sql.items(), tokenized_sql.statement_family);
        const generated_statement = try parseGeneratedRawStatementAlloc(alloc, tokenized_sql.items(), raw_statement);
        return .{
            .tokenized_sql = tokenized_sql,
            .raw_statement = raw_statement,
            .generated_statement = generated_statement,
            .statement = parseStatement(raw_statement, generated_statement, &tokenized_sql),
        };
    }

    pub fn initFromTokenSliceAlloc(alloc: std.mem.Allocator, source_sql: []const u8, source_tokens: []const Token) !ParsedSql {
        var tokenized_sql = try TokenizedSql.initFromTokenSliceAlloc(alloc, source_sql, source_tokens);
        errdefer tokenized_sql.deinit(alloc);
        const raw_statement = try parseRawStatement(tokenized_sql.items(), tokenized_sql.statement_family);
        const generated_statement = try parseGeneratedRawStatementAlloc(alloc, tokenized_sql.items(), raw_statement);
        return .{
            .tokenized_sql = tokenized_sql,
            .raw_statement = raw_statement,
            .generated_statement = generated_statement,
            .statement = parseStatement(raw_statement, generated_statement, &tokenized_sql),
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
        const generated_statement = try parseGeneratedRawStatementAlloc(alloc, tokenized_sql.items(), raw_statement);
        return .{
            .tokenized_sql = tokenized_sql,
            .raw_statement = raw_statement,
            .generated_statement = generated_statement,
            .statement = parseStatement(raw_statement, generated_statement, &tokenized_sql),
        };
    }

    pub fn deinit(self: *ParsedSql, alloc: std.mem.Allocator) void {
        if (self.generated_statement) |*generated_statement| generated_statement.deinit(alloc);
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

    pub fn generatedStatementKind(self: *const ParsedSql) ?generated_parser.GeneratedSqlStatementKind {
        if (self.generated_statement) |statement| return statement.kind();
        return null;
    }
};

fn parseGeneratedRawStatementAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    raw_statement: RawSqlStatement,
) !?GeneratedRawSqlStatement {
    const result = generated_parser.parseGeneratedGateTokensAlloc(alloc, tokens) catch |err| switch (err) {
        error.UnsupportedSqlShape, error.UnexpectedToken => {
            if (allowsGeneratedGrammarFallback(tokens, raw_statement)) return null;
            return err;
        },
        else => return err,
    };
    if (result) |parsed| {
        return .{ .raw = raw_statement, .statement = parsed.statement, .ast = parsed.ast };
    }
    return null;
}

fn allowsGeneratedGrammarFallback(tokens: []const Token, raw_statement: RawSqlStatement) bool {
    if (raw_statement.token_start >= raw_statement.token_end or raw_statement.token_end > tokens.len) return false;
    if (tokens[raw_statement.token_end - 1].kind == .eq or tokens[raw_statement.token_end - 1].kind == .comma) return false;
    if (tokenMatchesKeyword(tokens[raw_statement.token_end - 1], .to) or tokenMatchesKeyword(tokens[raw_statement.token_end - 1], .as)) return false;

    const first = tokens[raw_statement.token_start];
    if (tokenMatchesKeyword(first, .set)) return raw_statement.token_end > raw_statement.token_start + 2;
    if (tokenMatchesKeyword(first, .reset) or tokenMatchesKeyword(first, .show) or tokenMatchesKeyword(first, .discard)) return raw_statement.token_end > raw_statement.token_start + 1;
    if (tokenMatchesKeyword(first, .prepare)) return raw_statement.token_end > raw_statement.token_start + 2;
    if (tokenMatchesKeyword(first, .execute) or tokenMatchesKeyword(first, .deallocate)) return raw_statement.token_end > raw_statement.token_start + 1;
    if (tokenMatchesKeyword(first, .commit) or tokenMatchesKeyword(first, .rollback)) return raw_statement.token_end > raw_statement.token_start + 1;
    if (tokenMatchesText(first, "start") or tokenMatchesText(first, "lock")) return raw_statement.token_end > raw_statement.token_start + 1;
    if (tokenMatchesKeyword(first, .begin)) return true;
    if (tokenMatchesKeyword(first, .savepoint)) return raw_statement.token_end > raw_statement.token_start + 1;
    if (tokenMatchesText(first, "release")) return raw_statement.token_end > raw_statement.token_start + 1;
    if (tokenMatchesText(first, "declare") or tokenMatchesText(first, "close") or tokenMatchesText(first, "fetch") or tokenMatchesText(first, "move")) {
        return raw_statement.token_end > raw_statement.token_start + 1;
    }
    return switch (raw_statement.family orelse return false) {
        .insert, .update, .delete, .truncate, .merge, .ddl => true,
        .select, .with => false,
    };
}

fn parseStatement(
    raw_statement: RawSqlStatement,
    generated_statement: ?GeneratedRawSqlStatement,
    tokenized_sql: *const TokenizedSql,
) ParsedStatement {
    if (generated_statement) |generated_raw| {
        switch (generated_raw.statement) {
            .session => return .{ .session = .{ .raw = raw_statement } },
            .transaction => return .{ .transaction = .{ .raw = raw_statement } },
            .prepared => return .{ .prepared = .{ .raw = raw_statement } },
            .ddl => return .{ .ddl = .{ .raw = raw_statement } },
            .dml => if (tokenized_sql.write_statement_kind) |kind| return .{ .write = .{ .kind = kind, .raw = raw_statement } },
            .read => if (tokenized_sql.read_statement_kind) |kind| return .{ .read = .{ .kind = kind, .raw = raw_statement } },
            .graph => return .{ .ddl = .{ .raw = raw_statement } },
            .unsupported => {},
            .other => {},
        }
    }
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
        return .{ .prepared = .{ .raw = raw_statement } };
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

fn tokenMatchesKeyword(token: Token, keyword: TokenKeyword) bool {
    return token.matchesKeywordTag(keyword);
}

fn tokenMatchesText(token: Token, text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(token.text, text);
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

test "sql adapter parsed sql requires generated grammar for first migrated control family" {
    const alloc = std.testing.allocator;

    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SET search_path TO"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "PREPARE read_stmt AS"));

    var complex_ddl = try ParsedSql.initAlloc(alloc, "ALTER TABLE audit_log ALTER COLUMN amount TYPE numeric USING amount + 1;");
    defer complex_ddl.deinit(alloc);
    try std.testing.expect(complex_ddl.generated_statement == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .ddl), std.meta.activeTag(complex_ddl.statement));
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
            try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.unsupported, explain.generatedStatementKind().?);
            switch (explain.generated_statement.?.ast.?) {
                .unsupported => |unsupported| {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlUnsupportedKind.explain, unsupported.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlUnsupportedReason.explain_not_planned_by_generated_parser, unsupported.reason);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 5 }, unsupported.subject_tokens.?);
                },
                else => return error.TestUnexpectedResult,
            }
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
            try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.unsupported, explain_options.generatedStatementKind().?);
            switch (explain_options.generated_statement.?.ast.?) {
                .unsupported => |unsupported| {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlUnsupportedKind.explain, unsupported.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlUnsupportedReason.explain_not_planned_by_generated_parser, unsupported.reason);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = statement.inner_token_start.?, .end = statement.inner_token_end.? }, unsupported.subject_tokens.?);
                },
                else => return error.TestUnexpectedResult,
            }
        },
        else => return error.TestUnexpectedResult,
    }

    var explain_analyze = try ParsedSql.initAlloc(alloc, "EXPLAIN ANALYZE INSERT INTO usage_records (id) VALUES ('u1')");
    defer explain_analyze.deinit(alloc);
    switch (explain_analyze.statement) {
        .explain => |statement| {
            try std.testing.expect(statement.analyze);
            try std.testing.expectEqualStrings("INSERT", explain_analyze.items()[statement.inner_token_start.?].text);
            try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.unsupported, explain_analyze.generatedStatementKind().?);
            switch (explain_analyze.generated_statement.?.ast.?) {
                .unsupported => |unsupported| {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlUnsupportedKind.explain, unsupported.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlUnsupportedReason.explain_not_planned_by_generated_parser, unsupported.reason);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = statement.inner_token_start.?, .end = statement.inner_token_end.? }, unsupported.subject_tokens.?);
                },
                else => return error.TestUnexpectedResult,
            }
        },
        else => return error.TestUnexpectedResult,
    }

    var invalid_explain = try ParsedSql.initAlloc(alloc, "EXPLAIN (FORMAT YAML) SELECT 1");
    defer invalid_explain.deinit(alloc);
    switch (invalid_explain.statement) {
        .explain => |statement| {
            try std.testing.expect(statement.inner_token_start == null);
            try std.testing.expect(statement.inner_token_end == null);
            try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.unsupported, invalid_explain.generatedStatementKind().?);
            switch (invalid_explain.generated_statement.?.ast.?) {
                .unsupported => |unsupported| {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlUnsupportedKind.explain, unsupported.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlUnsupportedReason.explain_not_planned_by_generated_parser, unsupported.reason);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 7 }, unsupported.subject_tokens.?);
                },
                else => return error.TestUnexpectedResult,
            }
        },
        else => return error.TestUnexpectedResult,
    }

    var empty_explain = try ParsedSql.initAlloc(alloc, "EXPLAIN");
    defer empty_explain.deinit(alloc);
    switch (empty_explain.statement) {
        .explain => |statement| {
            try std.testing.expect(statement.inner_token_start == null);
            try std.testing.expect(statement.inner_token_end == null);
            try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.unsupported, empty_explain.generatedStatementKind().?);
            switch (empty_explain.generated_statement.?.ast.?) {
                .unsupported => |unsupported| {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlUnsupportedKind.explain, unsupported.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlUnsupportedReason.explain_not_planned_by_generated_parser, unsupported.reason);
                    try std.testing.expect(unsupported.subject_tokens == null);
                },
                else => return error.TestUnexpectedResult,
            }
        },
        else => return error.TestUnexpectedResult,
    }

    const unsupported_diagnostics = [_]struct {
        sql: []const u8,
        kind: generated_parser.GeneratedSqlUnsupportedKind,
        reason: generated_parser.GeneratedSqlUnsupportedReason,
    }{
        .{
            .sql = "COPY usage_records (id, status) FROM STDIN WITH (FORMAT csv)",
            .kind = .copy,
            .reason = .copy_not_planned_by_generated_parser,
        },
        .{
            .sql = "VACUUM (FULL, VERBOSE, ANALYZE) public.usage_records",
            .kind = .vacuum,
            .reason = .vacuum_not_planned_by_generated_parser,
        },
        .{
            .sql = "REINDEX INDEX CONCURRENTLY public.usage_status_idx",
            .kind = .reindex,
            .reason = .reindex_not_planned_by_generated_parser,
        },
    };
    for (unsupported_diagnostics) |case| {
        var parsed = try ParsedSql.initAlloc(alloc, case.sql);
        defer parsed.deinit(alloc);
        try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.unsupported, parsed.generatedStatementKind().?);
        switch (parsed.generated_statement.?.ast.?) {
            .unsupported => |unsupported| {
                try std.testing.expectEqual(case.kind, unsupported.kind);
                try std.testing.expectEqual(case.reason, unsupported.reason);
                try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = parsed.items().len }, unsupported.subject_tokens.?);
            },
            else => return error.TestUnexpectedResult,
        }
    }

    var session = try ParsedSql.initAlloc(alloc, "SET search_path TO public");
    defer session.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.session, session.generatedStatementKind().?);
    switch (session.generated_statement.?.ast.?) {
        .session => |generated_session| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlSessionKind.set, generated_session.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, generated_session.name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, generated_session.value_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (session.statement) {
        .session => {},
        else => return error.TestUnexpectedResult,
    }

    var prepared = try ParsedSql.initAlloc(alloc, "PREPARE read_stmt AS SELECT id FROM usage_records");
    defer prepared.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.prepared, prepared.generatedStatementKind().?);
    switch (prepared.statement) {
        .prepared => |statement| try std.testing.expectEqualStrings("PREPARE read_stmt AS SELECT id FROM usage_records", statement.raw.sql(prepared.sql())),
        else => return error.TestUnexpectedResult,
    }

    var ddl = try ParsedSql.initAlloc(alloc, "CREATE TABLE usage_records (id text)");
    defer ddl.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, ddl.generatedStatementKind().?);
    switch (ddl.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var unsupported_generated = try ParsedSql.initAlloc(alloc, "SELECT id FROM docs WHERE status = 'active' LIMIT 5");
    defer unsupported_generated.deinit(alloc);
    try std.testing.expect(unsupported_generated.generated_statement == null);
}

test "sql adapter parsed sql retains generated DML nodes for covered write corpus" {
    const alloc = std.testing.allocator;

    const cases = [_]struct {
        sql: []const u8,
        generated: generated_parser.GeneratedSqlDmlKind,
        write: classifier.SqlWriteStatementKind,
    }{
        .{ .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'open')", .generated = .insert_values, .write = .insert },
        .{ .sql = "INSERT INTO usage_records (id) SELECT id FROM incoming_usage", .generated = .insert_select, .write = .insert_source },
        .{ .sql = "UPDATE usage_records SET status = 'done' WHERE id = 'u1'", .generated = .update, .write = .update },
        .{ .sql = "DELETE FROM usage_records WHERE id = 'u1'", .generated = .delete, .write = .delete },
        .{ .sql = "TRUNCATE usage_records", .generated = .truncate, .write = .truncate },
        .{ .sql = "MERGE INTO usage_records USING source_rows ON usage_records.id = source_rows.id WHEN MATCHED THEN UPDATE SET status = source_rows.status", .generated = .merge, .write = .merge },
    };

    for (cases) |case| {
        var parsed = try ParsedSql.initAlloc(alloc, case.sql);
        defer parsed.deinit(alloc);
        try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.dml, parsed.generatedStatementKind().?);
        try std.testing.expectEqual(case.write, parsed.writeStatementKind().?);
        switch (parsed.generated_statement.?.statement) {
            .dml => |kind| try std.testing.expectEqual(case.generated, kind),
            else => return error.TestUnexpectedResult,
        }
        switch (parsed.generated_statement.?.ast.?) {
            .dml => |dml_ast| try std.testing.expectEqual(case.generated, dml_ast.kind),
            else => return error.TestUnexpectedResult,
        }
        switch (parsed.statement) {
            .write => |statement| try std.testing.expectEqual(case.write, statement.kind),
            else => return error.TestUnexpectedResult,
        }
    }
}

test "sql adapter parsed sql retains generated read nodes for covered query corpus" {
    const alloc = std.testing.allocator;

    const cases = [_]struct {
        sql: []const u8,
        generated: generated_parser.GeneratedSqlReadKind,
        read: classifier.SqlReadStatementKind,
    }{
        .{ .sql = "SELECT id, status FROM usage_records WHERE status = 'open' ORDER BY id LIMIT 10", .generated = .query, .read = .query },
        .{ .sql = "SELECT status AS state, id FROM usage_records", .generated = .query, .read = .query },
        .{ .sql = "SELECT status state, id FROM usage_records", .generated = .query, .read = .query },
        .{ .sql = "SELECT CAST(id AS text) AS id_text FROM usage_records WHERE id = 'u1'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE CAST(amount + 1 AS text) = '2'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id::text AS id_text FROM usage_records WHERE id::text = 'u1'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE metadata->'flags' = $1::jsonb", .generated = .query, .read = .query },
        .{ .sql = "SELECT metadata #>> '{billing,plan}' AS plan FROM usage_records WHERE metadata #> '{flags}' = $1::jsonb", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE metadata #>> '{billing,plan}' = 'pro'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status = ANY($1::text[])", .generated = .query, .read = .query },
        .{ .sql = "SELECT date_bin(INTERVAL '1 hour', amount, 0) AS amount_bucket FROM usage_records WHERE date_bin(INTERVAL '1 day', amount, 0) = $1", .generated = .query, .read = .query },
        .{ .sql = "SELECT date_bin(INTERVAL '1 hour', TIMESTAMPTZ '2025-01-01T01:30:00+01:30', TIMESTAMP '2025-01-01T00:00:00') AS planned_bucket FROM usage_records WHERE id = $1", .generated = .query, .read = .query },
        .{ .sql = "SELECT EXTRACT(dow FROM amount) AS amount_dow FROM usage_records WHERE EXTRACT(hour FROM amount) = $1", .generated = .query, .read = .query },
        .{ .sql = "SELECT date_part('hour', amount) AS amount_hour, EXTRACT(dow FROM amount) AS amount_dow FROM usage_records WHERE EXTRACT(dow FROM amount) = $1 ORDER BY date_part('month', amount) ASC LIMIT 5", .generated = .query, .read = .query },
        .{ .sql = "SELECT CURRENT_TIMESTAMP(6) AS planned_at_ns FROM users WHERE id = $1", .generated = .query, .read = .query },
        .{ .sql = "SELECT CURRENT_DATE AS planned_day_ns FROM users WHERE id = $1", .generated = .query, .read = .query },
        .{ .sql = "SELECT lower(p.valid_at) AS valid_start, upper(p.valid_at) AS valid_end FROM price_intervals AS p WHERE lower(p.valid_at) >= 1 AND upper(p.valid_at) IS NOT NULL ORDER BY upper(p.valid_at) DESC LIMIT 5", .generated = .query, .read = .query },
        .{ .sql = "SELECT CASE WHEN email IS NULL THEN 'missing' WHEN email = 'blocked@example.test' THEN 'blocked' ELSE lower(status) END AS email_bucket FROM usage_records WHERE id = 'u1'", .generated = .query, .read = .query },
        .{ .sql = "SELECT CASE WHEN email IS NULL THEN NULL ELSE email END AS maybe_email FROM usage_records WHERE id = 'u1'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status LIKE 'open%'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status ILIKE 'open%'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status LIKE 'op!_%' ESCAPE '!'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE lower(status) ILIKE 'op!_%' ESCAPE '!'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE lower(status) LIKE ANY(ARRAY['op%', 'ready%'])", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status LIKE SOME(ARRAY['op%', 'ready%'])", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE name ILIKE ALL(ARRAY['ada%', 'grace%'])", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE id IN ('u1', 'u2')", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE score BETWEEN 1 AND 10", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE priority BETWEEN SYMMETRIC 20 AND 10", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE array_length(tags, 1) BETWEEN SYMMETRIC 3 AND 1", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status NOT LIKE 'closed%'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status NOT ILIKE 'closed%'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status NOT LIKE 'cl!_%' ESCAPE '!'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE lower(status) NOT ILIKE 'cl!_%' ESCAPE '!'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE lower(name) NOT ILIKE ALL(ARRAY['bot%', 'sys%'])", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE id NOT IN ('u1', 'u2')", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE score NOT BETWEEN 1 AND 10", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE priority NOT BETWEEN ASYMMETRIC 10 AND 20", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE priority NOT BETWEEN SYMMETRIC 20 AND 10", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE score = ANY (1, 2)", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE score <> ALL (1, 2)", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE score > SOME (1, 2)", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status = ANY(ARRAY['active','pending']::text[])", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE score = ANY (SELECT score FROM thresholds WHERE active IS TRUE)", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE score <> ALL (SELECT score FROM archived_thresholds)", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE tags @> ARRAY['hot','new']", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE tags && ARRAY['hot','new']", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE metadata ? 'flags'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE metadata ?| ARRAY['flags','billing']", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE metadata ?& ARRAY['flags','billing']", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status ~ 'op.*'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status ~* 'op.*'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status !~ 'closed.*'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status !~* 'closed.*'", .generated = .query, .read = .query },
        .{ .sql = "SELECT first_name || ' ' || last_name FROM usage_records", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status || ':' || id = 'open:u1'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE deleted_at IS NULL", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE deleted_at IS NOT NULL", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status ISNULL", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE lower(status) NOTNULL", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE active IS TRUE", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE active IS NOT FALSE", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE active IS UNKNOWN", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE active IS NOT UNKNOWN", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status IS DISTINCT FROM previous_status", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status IS NOT DISTINCT FROM previous_status", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status = 'open' OR deleted_at IS NULL", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status = 'open' AND deleted_at IS NULL", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE NOT deleted_at IS NULL", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE (status = 'open')", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE NOT (deleted_at IS NULL)", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE score + bonus > 10", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE score * weight > 10", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE payload ->> 'status' = 'open'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE lower(status) = 'open'", .generated = .query, .read = .query },
        .{ .sql = "SELECT concat_ws(',', status), id FROM usage_records ORDER BY status, id", .generated = .query, .read = .query },
        .{ .sql = "SELECT customer, COUNT(*) FILTER (WHERE status = 'open') AS open_count FROM usage_records GROUP BY customer", .generated = .aggregate, .read = .aggregate },
        .{ .sql = "SELECT customer, COUNT(DISTINCT status) AS status_count FROM usage_records GROUP BY customer", .generated = .aggregate, .read = .aggregate },
        .{ .sql = "SELECT customer, ARRAY_AGG(DISTINCT status ORDER BY amount DESC) AS statuses FROM usage_records GROUP BY customer", .generated = .aggregate, .read = .aggregate },
        .{ .sql = "SELECT customer, percentile_cont(0.5) WITHIN GROUP (ORDER BY amount DESC NULLS LAST) AS median_amount FROM usage_records GROUP BY customer", .generated = .aggregate, .read = .aggregate },
        .{ .sql = "SELECT id, row_number() OVER (PARTITION BY tenant, account ORDER BY id) AS rn FROM usage_records ORDER BY id, tenant", .generated = .window, .read = .window },
        .{ .sql = "SELECT DISTINCT status FROM usage_records ORDER BY status", .generated = .aggregate, .read = .aggregate },
        .{ .sql = "SELECT DISTINCT ON (organization_id) organization_id, id FROM usage_records ORDER BY organization_id ASC, created_at DESC", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records ORDER BY id LIMIT ALL OFFSET 2 ROWS", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records ORDER BY created_at DESC NULLS LAST, score ASC NULLS FIRST", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records ORDER BY 1 USING > LIMIT 5", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records FETCH FIRST ROWS ONLY", .generated = .query, .read = .query },
        .{ .sql = "SELECT status FROM usage_records GROUP BY status HAVING status = 'open'", .generated = .aggregate, .read = .aggregate },
        .{ .sql = "SELECT usage_records.id FROM usage_records JOIN accounts ON usage_records.account_id = accounts.id", .generated = .join, .read = .join },
        .{ .sql = "SELECT usage_records.id FROM usage_records JOIN accounts ON usage_records.account_id = accounts.id JOIN tenants ON accounts.tenant_id = tenants.id", .generated = .join, .read = .join },
        .{ .sql = "SELECT usage_records.id FROM usage_records LEFT OUTER JOIN accounts ON usage_records.account_id = accounts.id", .generated = .join, .read = .join },
        .{ .sql = "SELECT id FROM LATERAL (SELECT id FROM usage_records) AS source_rows", .generated = .lateral, .read = .lateral },
        .{ .sql = "SELECT id, row_number() OVER (ORDER BY id) AS rn FROM usage_records", .generated = .window, .read = .window },
        .{ .sql = "SELECT id, row_number() OVER (PARTITION BY tenant ORDER BY id) AS rn FROM usage_records", .generated = .window, .read = .window },
        .{ .sql = "SELECT id, row_number() OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rn FROM usage_records", .generated = .window, .read = .window },
        .{ .sql = "SELECT id, row_number() OVER usage_window AS rn FROM usage_records WINDOW usage_window AS (ORDER BY id)", .generated = .window, .read = .window },
        .{ .sql = "SELECT id, row_number() OVER usage_window AS rn FROM usage_records WINDOW usage_window AS (PARTITION BY tenant ORDER BY id)", .generated = .window, .read = .window },
        .{ .sql = "SELECT id, row_number() OVER usage_window AS rn FROM usage_records WINDOW usage_window AS (ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)", .generated = .window, .read = .window },
        .{ .sql = "SELECT id FROM usage_records UNION SELECT id FROM usage_archive", .generated = .set_operation, .read = .set_operation },
        .{ .sql = "WITH source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows", .generated = .cte, .read = .query },
        .{ .sql = "WITH source_rows AS MATERIALIZED (SELECT id FROM usage_records) SELECT id FROM source_rows", .generated = .cte, .read = .query },
        .{ .sql = "WITH source_rows(source_id) AS NOT MATERIALIZED (SELECT id FROM usage_records) SELECT source_id FROM source_rows", .generated = .cte, .read = .query },
        .{ .sql = "WITH first_rows AS (SELECT id FROM usage_records), second_rows AS (SELECT id FROM first_rows) SELECT id FROM second_rows", .generated = .cte, .read = .query },
        .{ .sql = "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows", .generated = .cte, .read = .recursive_cte },
    };

    for (cases) |case| {
        var parsed = try ParsedSql.initAlloc(alloc, case.sql);
        defer parsed.deinit(alloc);
        try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, parsed.generatedStatementKind().?);
        try std.testing.expectEqual(case.read, parsed.readStatementKind().?);
        switch (parsed.generated_statement.?.statement) {
            .read => |kind| try std.testing.expectEqual(case.generated, kind),
            else => return error.TestUnexpectedResult,
        }
        switch (parsed.generated_statement.?.ast.?) {
            .read => |read_ast| {
                try std.testing.expectEqual(case.generated, read_ast.kind);
                if (std.mem.eql(u8, case.sql, "SELECT id, status FROM usage_records WHERE status = 'open' ORDER BY id LIMIT 10")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 4 }, read_ast.projection_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.source_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 10 }, read_ast.where_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read_ast.order_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 14, .end = 15 }, read_ast.limit_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.limit_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 14, .end = 15 }, read_ast.limit_expression.tokens.?);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.count);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.items.len);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read_ast.projection_items.items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.projection_items.items[1]);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.expressions.len);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.projection_items.expressions[0].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.projection_items.expressions[1].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.projection_first_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read_ast.projection_first_expression.tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.projection_last_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.projection_last_expression.tokens.?);
                    try std.testing.expectEqual(@as(usize, 1), read_ast.order_items.count);
                    try std.testing.expectEqual(@as(usize, 1), read_ast.order_items.items.len);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read_ast.order_items.items[0]);
                    try std.testing.expectEqual(@as(usize, 1), read_ast.order_items.expressions.len);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.order_items.expressions[0].kind);
                } else if (std.mem.eql(u8, case.sql, "SELECT status AS state, id FROM usage_records")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 6 }, read_ast.projection_tokens.?);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 4 }, read_ast.projection_items.items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read_ast.projection_items.expression_items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 4 }, read_ast.projection_items.alias_items[0].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.projection_items.alias_name_items[0].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.projection_items.items[1]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.projection_items.expression_items[1]);
                    try std.testing.expect(read_ast.projection_items.alias_items[1] == null);
                    try std.testing.expect(read_ast.projection_items.alias_name_items[1] == null);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.projection_items.expressions[0].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read_ast.projection_items.expressions[0].tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT status state, id FROM usage_records")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 5 }, read_ast.projection_tokens.?);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 3 }, read_ast.projection_items.items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read_ast.projection_items.expression_items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, read_ast.projection_items.alias_items[0].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, read_ast.projection_items.alias_name_items[0].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read_ast.projection_items.items[1]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read_ast.projection_items.expression_items[1]);
                    try std.testing.expect(read_ast.projection_items.alias_items[1] == null);
                    try std.testing.expect(read_ast.projection_items.alias_name_items[1] == null);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.projection_items.expressions[0].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read_ast.projection_items.expressions[0].tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT CAST(id AS text) AS id_text FROM usage_records WHERE id = 'u1'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 9 }, read_ast.projection_tokens.?);
                    try std.testing.expectEqual(@as(usize, 1), read_ast.projection_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.cast, read_ast.projection_items.expressions[0].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.projection_items.expressions[0].cast_expression_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.projection_items.expressions[0].cast_type_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.projection_items.alias_name_items[0].?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE CAST(amount + 1 AS text) = '2'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.cast, read_ast.where_expression.left_expression_kind.?);
                    const cast_expression = read_ast.where_expression.left_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 10 }, cast_expression.cast_expression_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.additive, cast_expression.cast_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 11, .end = 12 }, cast_expression.cast_type_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id::text AS id_text FROM usage_records WHERE id::text = 'u1'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.projection_items.expressions[0].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read_ast.projection_items.expressions[0].tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expect(read_ast.where_expression.left_expression_kind == null);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE metadata->'flags' = $1::jsonb")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.json_access, read_ast.where_expression.left_expression_kind.?);
                    try std.testing.expect(read_ast.where_expression.right_expression_kind == null);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status = ANY($1::text[])")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.quantified_comparison, read_ast.where_expression.kind);
                    const grouped = read_ast.where_expression.right_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, grouped.inner_expression.?.kind);
                } else if (std.mem.eql(u8, case.sql, "SELECT date_bin(INTERVAL '1 hour', amount, 0) AS amount_bucket FROM usage_records WHERE date_bin(INTERVAL '1 day', amount, 0) = $1")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.projection_first_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 9 }, read_ast.projection_first_expression.argument_tokens.?);
                    try std.testing.expectEqual(@as(usize, 3), read_ast.projection_first_expression.argument_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.interval_literal, read_ast.projection_first_expression.argument_items.expressions[0].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read_ast.projection_first_expression.argument_items.expressions[0].interval_value_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.where_expression.left_expression_kind.?);
                    const predicate_call = read_ast.where_expression.left_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, predicate_call.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 17, .end = 23 }, predicate_call.argument_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.interval_literal, predicate_call.argument_items.expressions[0].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 18, .end = 19 }, predicate_call.argument_items.expressions[0].interval_value_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 24, .end = 25 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 25, .end = 26 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT date_bin(INTERVAL '1 hour', TIMESTAMPTZ '2025-01-01T01:30:00+01:30', TIMESTAMP '2025-01-01T00:00:00') AS planned_bucket FROM usage_records WHERE id = $1")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.projection_first_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 11 }, read_ast.projection_first_expression.argument_tokens.?);
                    try std.testing.expectEqual(@as(usize, 3), read_ast.projection_first_expression.argument_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.interval_literal, read_ast.projection_first_expression.argument_items.expressions[0].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.timestamp_literal, read_ast.projection_first_expression.argument_items.expressions[1].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.projection_first_expression.argument_items.expressions[1].timestamp_type_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.projection_first_expression.argument_items.expressions[1].timestamp_value_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.timestamp_literal, read_ast.projection_first_expression.argument_items.expressions[2].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.projection_first_expression.argument_items.expressions[2].timestamp_type_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read_ast.projection_first_expression.argument_items.expressions[2].timestamp_value_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 18, .end = 19 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 19, .end = 20 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT EXTRACT(dow FROM amount) AS amount_dow FROM usage_records WHERE EXTRACT(hour FROM amount) = $1")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.extract_expression, read_ast.projection_first_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.projection_first_expression.extract_field_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.projection_first_expression.extract_source_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.extract_expression, read_ast.where_expression.left_expression_kind.?);
                    const predicate_extract = read_ast.where_expression.left_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.extract_expression, predicate_extract.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 14, .end = 15 }, predicate_extract.extract_field_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 16, .end = 17 }, predicate_extract.extract_source_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 18, .end = 19 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 19, .end = 20 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT date_part('hour', amount) AS amount_hour, EXTRACT(dow FROM amount) AS amount_dow FROM usage_records WHERE EXTRACT(dow FROM amount) = $1 ORDER BY date_part('month', amount) ASC LIMIT 5")) {
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.projection_items.expressions[0].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read_ast.projection_items.expressions[0].function_name_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 6 }, read_ast.projection_items.expressions[0].argument_tokens.?);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.expressions[0].argument_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.extract_expression, read_ast.projection_items.expressions[1].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read_ast.projection_items.expressions[1].extract_field_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 14, .end = 15 }, read_ast.projection_items.expressions[1].extract_source_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.extract_expression, read_ast.where_expression.left_expression_kind.?);
                    const predicate_extract = read_ast.where_expression.left_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 23, .end = 24 }, predicate_extract.extract_field_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 25, .end = 26 }, predicate_extract.extract_source_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 27, .end = 28 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 28, .end = 29 }, read_ast.where_expression.right_tokens.?);
                    try std.testing.expectEqual(@as(usize, 1), read_ast.order_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.order_first_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 31, .end = 32 }, read_ast.order_first_expression.function_name_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 33, .end = 36 }, read_ast.order_first_expression.argument_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 37, .end = 38 }, read_ast.order_items.direction_items[0].?);
                } else if (std.mem.eql(u8, case.sql, "SELECT CURRENT_TIMESTAMP(6) AS planned_at_ns FROM users WHERE id = $1")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.current_timestamp, read_ast.projection_first_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.projection_first_expression.current_timestamp_precision_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT CURRENT_DATE AS planned_day_ns FROM users WHERE id = $1")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.current_date, read_ast.projection_first_expression.kind);
                    try std.testing.expect(read_ast.projection_first_expression.current_timestamp_precision_tokens == null);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT lower(p.valid_at) AS valid_start, upper(p.valid_at) AS valid_end FROM price_intervals AS p WHERE lower(p.valid_at) >= 1 AND upper(p.valid_at) IS NOT NULL ORDER BY upper(p.valid_at) DESC LIMIT 5")) {
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.projection_items.expressions[0].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read_ast.projection_items.expressions[0].function_name_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.projection_items.expressions[0].argument_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.projection_items.expressions[1].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.projection_items.expressions[1].function_name_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read_ast.projection_items.expressions[1].argument_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 15, .end = 18 }, read_ast.source_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.logical_and, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.left_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_not_null, read_ast.where_expression.right_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.order_first_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 35, .end = 36 }, read_ast.order_first_expression.function_name_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 37, .end = 38 }, read_ast.order_first_expression.argument_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 39, .end = 40 }, read_ast.order_items.direction_items[0].?);
                } else if (std.mem.eql(u8, case.sql, "SELECT CASE WHEN email IS NULL THEN 'missing' WHEN email = 'blocked@example.test' THEN 'blocked' ELSE lower(status) END AS email_bucket FROM usage_records WHERE id = 'u1'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.case_expression, read_ast.projection_items.expressions[0].kind);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.expressions[0].case_branch_count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 8 }, read_ast.projection_items.expressions[0].case_first_when_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_null, read_ast.projection_items.expressions[0].case_first_condition_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 15, .end = 19 }, read_ast.projection_items.expressions[0].case_else_expression_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.projection_items.expressions[0].case_else_expression_kind.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT CASE WHEN email IS NULL THEN NULL ELSE email END AS maybe_email FROM usage_records WHERE id = 'u1'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.case_expression, read_ast.projection_items.expressions[0].kind);
                    try std.testing.expectEqual(@as(usize, 1), read_ast.projection_items.expressions[0].case_branch_count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.projection_items.expressions[0].case_first_result_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.projection_items.expressions[0].case_else_expression_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status LIKE 'open%'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.like, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status ILIKE 'open%'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.ilike, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status LIKE 'op!_%' ESCAPE '!'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.like, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 10 }, read_ast.where_expression.escape_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.escape_expression.?.tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE lower(status) ILIKE 'op!_%' ESCAPE '!'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.ilike, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read_ast.where_expression.right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 11, .end = 13 }, read_ast.where_expression.escape_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read_ast.where_expression.escape_expression.?.tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE lower(status) LIKE ANY(ARRAY['op%', 'ready%'])")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.like, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read_ast.where_expression.quantifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 11, .end = 19 }, read_ast.where_expression.right_tokens.?);
                    const grouped = read_ast.where_expression.right_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.grouped, grouped.kind);
                    const array_constructor = grouped.inner_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.array_constructor, array_constructor.kind);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status LIKE SOME(ARRAY['op%', 'ready%'])")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.like, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.quantifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 16 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE name ILIKE ALL(ARRAY['ada%', 'grace%'])")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.ilike, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.quantifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 16 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE id IN ('u1', 'u2')")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.in_list, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 12 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE score BETWEEN 1 AND 10")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.between, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 10 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE priority BETWEEN SYMMETRIC 20 AND 10")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.between, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.between_modifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlBetweenModifier.symmetric, read_ast.where_expression.between_modifier.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 11 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE array_length(tags, 1) BETWEEN SYMMETRIC 3 AND 1")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.between, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 11 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.where_expression.left_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read_ast.where_expression.between_modifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlBetweenModifier.symmetric, read_ast.where_expression.between_modifier.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 13, .end = 16 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status NOT LIKE 'closed%'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.not_like, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.negation_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.operator_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status NOT ILIKE 'closed%'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.not_ilike, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.negation_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.operator_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status NOT LIKE 'cl!_%' ESCAPE '!'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.not_like, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.negation_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.where_expression.right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 11 }, read_ast.where_expression.escape_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read_ast.where_expression.escape_expression.?.tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE lower(status) NOT ILIKE 'cl!_%' ESCAPE '!'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.not_ilike, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.negation_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read_ast.where_expression.right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 12, .end = 14 }, read_ast.where_expression.escape_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read_ast.where_expression.escape_expression.?.tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE lower(name) NOT ILIKE ALL(ARRAY['bot%', 'sys%'])")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.not_ilike, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.negation_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read_ast.where_expression.quantifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 12, .end = 20 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE id NOT IN ('u1', 'u2')")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.not_in_list, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.negation_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 13 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE score NOT BETWEEN 1 AND 10")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.not_between, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.negation_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 11 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE priority NOT BETWEEN ASYMMETRIC 10 AND 20")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.not_between, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.negation_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.where_expression.between_modifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlBetweenModifier.asymmetric, read_ast.where_expression.between_modifier.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE priority NOT BETWEEN SYMMETRIC 20 AND 10")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.not_between, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.negation_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.where_expression.between_modifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlBetweenModifier.symmetric, read_ast.where_expression.between_modifier.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE score = ANY (1, 2)")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.quantified_comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.quantifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 13 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE score <> ALL (1, 2)")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.quantified_comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.quantifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 13 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE score > SOME (1, 2)")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.quantified_comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.quantifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 13 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status = ANY(ARRAY['active','pending']::text[])")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.quantified_comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.quantifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 16 }, read_ast.where_expression.right_tokens.?);
                    const grouped = read_ast.where_expression.right_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.grouped, grouped.kind);
                    const array_constructor = grouped.inner_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.array_constructor, array_constructor.kind);
                    try std.testing.expectEqual(@as(usize, 2), array_constructor.array_items.count);
                    try std.testing.expectEqual(@as(usize, 2), array_constructor.array_items.expressions.len);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE score = ANY (SELECT score FROM thresholds WHERE active IS TRUE)")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.quantified_comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.quantifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 18 }, read_ast.where_expression.right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.subquery, read_ast.where_expression.right_expression_kind.?);
                    const subquery = read_ast.where_expression.right_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.subquery, subquery.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 17 }, subquery.inner_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE score <> ALL (SELECT score FROM archived_thresholds)")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.quantified_comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.quantifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 14 }, read_ast.where_expression.right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.subquery, read_ast.where_expression.right_expression_kind.?);
                    const subquery = read_ast.where_expression.right_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.subquery, subquery.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 13 }, subquery.inner_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE tags @> ARRAY['hot','new']")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.contains, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 13 }, read_ast.where_expression.right_tokens.?);
                    const array_constructor = read_ast.where_expression.right_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.array_constructor, array_constructor.kind);
                    try std.testing.expectEqual(@as(usize, 2), array_constructor.array_items.count);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE tags && ARRAY['hot','new']")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.overlaps, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 13 }, read_ast.where_expression.right_tokens.?);
                    const array_constructor = read_ast.where_expression.right_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.array_constructor, array_constructor.kind);
                    try std.testing.expectEqual(@as(usize, 2), array_constructor.array_items.count);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE metadata ? 'flags'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.json_key_exists, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE metadata ?| ARRAY['flags','billing']")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.json_key_any, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 13 }, read_ast.where_expression.right_tokens.?);
                    const array_constructor = read_ast.where_expression.right_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.array_constructor, array_constructor.kind);
                    try std.testing.expectEqual(@as(usize, 2), array_constructor.array_items.count);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE metadata ?& ARRAY['flags','billing']")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.json_key_all, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 13 }, read_ast.where_expression.right_tokens.?);
                    const array_constructor = read_ast.where_expression.right_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.array_constructor, array_constructor.kind);
                    try std.testing.expectEqual(@as(usize, 2), array_constructor.array_items.count);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status ~ 'op.*'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.regex_match, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status ~* 'op.*'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.regex_imatch, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status !~ 'closed.*'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.regex_not_match, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status !~* 'closed.*'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.regex_not_imatch, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT first_name || ' ' || last_name FROM usage_records")) {
                    try std.testing.expectEqual(@as(usize, 1), read_ast.projection_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.string_concat, read_ast.projection_items.expressions[0].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read_ast.projection_items.expressions[0].left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, read_ast.projection_items.expressions[0].operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 6 }, read_ast.projection_items.expressions[0].right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.string_concat, read_ast.projection_items.expressions[0].right_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.string_concat, read_ast.projection_first_expression.kind);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status || ':' || id = 'open:u1'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 10 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.string_concat, read_ast.where_expression.left_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE deleted_at IS NULL")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_null, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE deleted_at IS NOT NULL")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_not_null, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 9 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status ISNULL")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_null, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expect(read_ast.where_expression.right_tokens == null);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE lower(status) NOTNULL")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_not_null, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.where_expression.left_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expect(read_ast.where_expression.right_tokens == null);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE active IS TRUE")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_true, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE active IS NOT FALSE")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_not_false, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 9 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE active IS UNKNOWN")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_unknown, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE active IS NOT UNKNOWN")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_not_unknown, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 9 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status IS DISTINCT FROM previous_status")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_distinct_from, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 9 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status IS NOT DISTINCT FROM previous_status")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_not_distinct_from, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.negation_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 10 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status = 'open' OR deleted_at IS NULL")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.logical_or, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.left_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.left_expression.?.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read_ast.where_expression.left_expression.?.tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read_ast.where_expression.right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_null, read_ast.where_expression.right_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_null, read_ast.where_expression.right_expression.?.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read_ast.where_expression.right_expression.?.tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status = 'open' AND deleted_at IS NULL")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.logical_and, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.left_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read_ast.where_expression.right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_null, read_ast.where_expression.right_expression_kind.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE NOT deleted_at IS NULL")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.logical_not, read_ast.where_expression.kind);
                    try std.testing.expect(read_ast.where_expression.left_tokens == null);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 9 }, read_ast.where_expression.right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_null, read_ast.where_expression.right_expression_kind.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE (status = 'open')")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.grouped, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 9 }, read_ast.where_expression.inner_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.inner_expression_kind.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE NOT (deleted_at IS NULL)")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.logical_not, read_ast.where_expression.kind);
                    try std.testing.expect(read_ast.where_expression.left_tokens == null);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 11 }, read_ast.where_expression.right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.grouped, read_ast.where_expression.right_expression_kind.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE score + bonus > 10")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.additive, read_ast.where_expression.left_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE score * weight > 10")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.multiplicative, read_ast.where_expression.left_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE payload ->> 'status' = 'open'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.json_text_access, read_ast.where_expression.left_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT metadata #>> '{billing,plan}' AS plan FROM usage_records WHERE metadata #> '{flags}' = $1::jsonb")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 6 }, read_ast.projection_tokens.?);
                    try std.testing.expectEqual(@as(usize, 1), read_ast.projection_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.json_path_text_access, read_ast.projection_first_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 4 }, read_ast.projection_items.expression_items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 6 }, read_ast.projection_items.alias_items[0].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.json_path_access, read_ast.where_expression.left_expression_kind.?);
                    const path_left = read_ast.where_expression.left_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.json_path_access, path_left.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, path_left.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 11, .end = 12 }, path_left.right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE metadata #>> '{billing,plan}' = 'pro'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.json_path_text_access, read_ast.where_expression.left_expression_kind.?);
                    const path_left = read_ast.where_expression.left_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.json_path_text_access, path_left.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, path_left.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, path_left.right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE lower(status) = 'open'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.where_expression.left_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT concat_ws(',', status), id FROM usage_records ORDER BY status, id")) {
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 7 }, read_ast.projection_items.first_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.projection_items.last_tokens.?);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.items.len);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 7 }, read_ast.projection_items.items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.projection_items.items[1]);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.expressions.len);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.projection_items.expressions[0].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.projection_items.expressions[1].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.projection_first_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read_ast.projection_first_expression.function_name_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 6 }, read_ast.projection_first_expression.argument_tokens.?);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_first_expression.argument_items.count);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_first_expression.argument_items.items.len);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.projection_first_expression.argument_items.items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.projection_first_expression.argument_items.items[1]);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_first_expression.argument_items.expressions.len);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.projection_last_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.projection_last_expression.tokens.?);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.order_items.count);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.order_items.items.len);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read_ast.order_items.items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 15, .end = 16 }, read_ast.order_items.items[1]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read_ast.order_items.first_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 15, .end = 16 }, read_ast.order_items.last_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.order_first_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read_ast.order_first_expression.tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.order_last_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 15, .end = 16 }, read_ast.order_last_expression.tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT customer, COUNT(*) FILTER (WHERE status = 'open') AS open_count FROM usage_records GROUP BY customer")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 16 }, read_ast.projection_tokens.?);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 16 }, read_ast.projection_items.items[1]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 14 }, read_ast.projection_items.expression_items[1]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 14, .end = 16 }, read_ast.projection_items.alias_items[1].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.projection_items.expressions[1].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.projection_items.expressions[1].function_name_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.projection_items.expressions[1].argument_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 14 }, read_ast.projection_items.expressions[1].filter_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 13 }, read_ast.projection_items.expressions[1].filter_predicate_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.projection_items.expressions[1].filter_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.projection_items.expressions[1].filter_expression.?.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 20, .end = 21 }, read_ast.group_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT customer, COUNT(DISTINCT status) AS status_count FROM usage_records GROUP BY customer")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 10 }, read_ast.projection_items.items[1]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.projection_items.expressions[1].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 7 }, read_ast.projection_items.expressions[1].argument_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.projection_items.expressions[1].argument_distinct_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.projection_items.expressions[1].argument_value_tokens.?);
                    try std.testing.expectEqual(@as(usize, 1), read_ast.projection_items.expressions[1].argument_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 14, .end = 15 }, read_ast.group_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT customer, ARRAY_AGG(DISTINCT status ORDER BY amount DESC) AS statuses FROM usage_records GROUP BY customer")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 14 }, read_ast.projection_items.items[1]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.projection_items.expressions[1].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 11 }, read_ast.projection_items.expressions[1].argument_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.projection_items.expressions[1].argument_distinct_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.projection_items.expressions[1].argument_value_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 11 }, read_ast.projection_items.expressions[1].argument_order_tokens.?);
                    try std.testing.expectEqual(@as(usize, 1), read_ast.projection_items.expressions[1].argument_order_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlOrderDirection.desc, read_ast.projection_items.expressions[1].argument_order_items.directions[0].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 18, .end = 19 }, read_ast.group_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT customer, percentile_cont(0.5) WITHIN GROUP (ORDER BY amount DESC NULLS LAST) AS median_amount FROM usage_records GROUP BY customer")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 19 }, read_ast.projection_tokens.?);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 19 }, read_ast.projection_items.items[1]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 17 }, read_ast.projection_items.expression_items[1]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.projection_items.expressions[1].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 17 }, read_ast.projection_items.expressions[1].within_group_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 12, .end = 16 }, read_ast.projection_items.expressions[1].within_group_order_tokens.?);
                    try std.testing.expectEqual(@as(usize, 1), read_ast.projection_items.expressions[1].within_group_order_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 12, .end = 16 }, read_ast.projection_items.expressions[1].within_group_order_items.items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlOrderDirection.desc, read_ast.projection_items.expressions[1].within_group_order_items.directions[0].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlNullsOrder.last, read_ast.projection_items.expressions[1].within_group_order_items.nulls_orders[0].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 23, .end = 24 }, read_ast.group_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id, row_number() OVER (PARTITION BY tenant, account ORDER BY id) AS rn FROM usage_records ORDER BY id, tenant")) {
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.count);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.items.len);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read_ast.projection_items.first_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 19 }, read_ast.projection_items.last_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read_ast.projection_items.expression_items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 17 }, read_ast.projection_items.expression_items[1]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 17, .end = 19 }, read_ast.projection_items.alias_items[1].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 18, .end = 19 }, read_ast.projection_items.alias_name_items[1].?);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.order_items.count);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.order_items.items.len);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 23, .end = 24 }, read_ast.order_items.first_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 25, .end = 26 }, read_ast.order_items.last_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 7 }, read_ast.offset_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.offset_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.offset_expression.tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 12 }, read_ast.fetch_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.fetch_count_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.fetch_count_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.fetch_count_expression.tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records ORDER BY id LIMIT ALL OFFSET 2 ROWS")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.order_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.limit_tokens.?);
                    try std.testing.expect(read_ast.limit_all);
                    try std.testing.expect(read_ast.limit_expression.tokens == null);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 12 }, read_ast.offset_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.offset_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read_ast.offset_expression.tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records FETCH FIRST ROWS ONLY")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read_ast.fetch_tokens.?);
                    try std.testing.expect(read_ast.fetch_count_tokens == null);
                    try std.testing.expect(read_ast.fetch_count_expression.tokens == null);
                } else if (std.mem.eql(u8, case.sql, "SELECT DISTINCT ON (organization_id) organization_id, id FROM usage_records ORDER BY organization_id ASC, created_at DESC")) {
                    try std.testing.expect(read_ast.distinct_tokens != null);
                    try std.testing.expect(read_ast.projection_tokens != null);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records ORDER BY created_at DESC NULLS LAST, score ASC NULLS FIRST")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 15 }, read_ast.order_tokens.?);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.order_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 10 }, read_ast.order_items.items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.order_items.expression_items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.order_items.direction_items[0].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlOrderDirection.desc, read_ast.order_items.directions[0].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 10 }, read_ast.order_items.nulls_order_items[0].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlNullsOrder.last, read_ast.order_items.nulls_orders[0].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 11, .end = 15 }, read_ast.order_items.items[1]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read_ast.order_items.expression_items[1]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read_ast.order_items.direction_items[1].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlOrderDirection.asc, read_ast.order_items.directions[1].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 13, .end = 15 }, read_ast.order_items.nulls_order_items[1].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlNullsOrder.first, read_ast.order_items.nulls_orders[1].?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records ORDER BY 1 USING > LIMIT 5")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 9 }, read_ast.order_tokens.?);
                    try std.testing.expectEqual(@as(usize, 1), read_ast.order_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 9 }, read_ast.order_items.items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.order_items.expression_items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 9 }, read_ast.order_items.direction_items[0].?);
                    try std.testing.expect(read_ast.order_items.directions[0] == null);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.order_items.order_using_operator_items[0].?);
                    try std.testing.expect(read_ast.order_items.nulls_order_items[0] == null);
                    try std.testing.expect(read_ast.order_items.nulls_orders[0] == null);
                } else if (case.generated == .join) {
                    if (std.mem.indexOf(u8, case.sql, " JOIN tenants ")) |_| {
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 16 }, read_ast.join_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read_ast.join_operator_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlJoinKind.inner, read_ast.join_kind.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.join_left_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.join_right_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 10 }, read_ast.join_predicate_tokens.?);
                        try std.testing.expectEqual(@as(usize, 2), read_ast.join_items.len);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 10 }, read_ast.join_items[0].tokens);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read_ast.join_items[0].operator_tokens);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlJoinKind.inner, read_ast.join_items[0].kind);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.join_items[0].left_tokens);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.join_items[0].right_tokens);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlJoinConditionKind.on, read_ast.join_items[0].condition_kind);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 10 }, read_ast.join_items[0].condition_tokens);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 10 }, read_ast.join_items[0].predicate_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.join_items[0].predicate_expression.kind);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 16 }, read_ast.join_items[1].tokens);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read_ast.join_items[1].operator_tokens);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlJoinKind.inner, read_ast.join_items[1].kind);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 10 }, read_ast.join_items[1].left_tokens);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read_ast.join_items[1].right_tokens);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlJoinConditionKind.on, read_ast.join_items[1].condition_kind);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 12, .end = 16 }, read_ast.join_items[1].condition_tokens);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 13, .end = 16 }, read_ast.join_items[1].predicate_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.join_items[1].predicate_expression.kind);
                    } else if (std.mem.indexOf(u8, case.sql, "LEFT OUTER")) |_| {
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 12 }, read_ast.join_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 7 }, read_ast.join_operator_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlJoinKind.left, read_ast.join_kind.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.join_left_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.join_right_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read_ast.join_predicate_tokens.?);
                        try std.testing.expectEqual(@as(usize, 1), read_ast.join_items.len);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 12 }, read_ast.join_items[0].tokens);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.join_predicate_expression.left_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read_ast.join_predicate_expression.operator_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read_ast.join_predicate_expression.right_tokens.?);
                    } else {
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 10 }, read_ast.join_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read_ast.join_operator_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlJoinKind.inner, read_ast.join_kind.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.join_left_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.join_right_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 10 }, read_ast.join_predicate_tokens.?);
                        try std.testing.expectEqual(@as(usize, 1), read_ast.join_items.len);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 10 }, read_ast.join_items[0].tokens);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.join_predicate_expression.left_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.join_predicate_expression.operator_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.join_predicate_expression.right_tokens.?);
                    }
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.join_predicate_expression.kind);
                } else if (case.generated == .aggregate) {
                    if (std.mem.indexOf(u8, case.sql, "DISTINCT")) |_| {
                        try std.testing.expect(read_ast.distinct_tokens != null);
                    } else {
                        try std.testing.expect(read_ast.group_tokens != null);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.group_first_expression.kind);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.group_first_expression.tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.group_last_expression.kind);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.group_last_expression.tokens.?);
                        try std.testing.expect(read_ast.having_tokens != null);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.having_expression.kind);
                    }
                } else if (case.generated == .window) {
                    try std.testing.expect(read_ast.projection_tokens != null);
                    try std.testing.expect(read_ast.source_tokens != null);
                    if (std.mem.indexOf(u8, case.sql, " WINDOW ")) |_| {
                        try std.testing.expect(read_ast.window_tokens != null);
                    }
                } else if (case.generated == .cte) {
                    try std.testing.expect(read_ast.cte_tokens != null);
                    try std.testing.expect(read_ast.cte_list_tokens != null);
                    try std.testing.expect(read_ast.cte_name_tokens != null);
                    try std.testing.expect(read_ast.cte_body_tokens != null);
                    try std.testing.expect(read_ast.cte_count > 0);
                    try std.testing.expectEqual(read_ast.cte_count, read_ast.cte_items.len);
                    try std.testing.expectEqual(read_ast.cte_name_tokens.?, read_ast.cte_items[0].name_tokens);
                    try std.testing.expectEqual(read_ast.cte_body_tokens.?, read_ast.cte_items[0].body_tokens.?);
                    if (std.mem.indexOf(u8, case.sql, " second_rows ")) |_| {
                        try std.testing.expectEqual(@as(usize, 2), read_ast.cte_count);
                        try std.testing.expect(read_ast.cte_last_name_tokens != null);
                        try std.testing.expect(read_ast.cte_last_body_tokens != null);
                        try std.testing.expectEqual(read_ast.cte_last_name_tokens.?, read_ast.cte_items[1].name_tokens);
                        try std.testing.expectEqual(read_ast.cte_last_body_tokens.?, read_ast.cte_items[1].body_tokens.?);
                    }
                    if (std.mem.indexOf(u8, case.sql, "WITH RECURSIVE")) |_| {
                        try std.testing.expect(read_ast.cte_recursive);
                    }
                    if (std.mem.indexOf(u8, case.sql, "AS MATERIALIZED")) |_| {
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.cte_items[0].materialization_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlCteMaterialization.materialized, read_ast.cte_items[0].materialization.?);
                    }
                    if (std.mem.indexOf(u8, case.sql, "AS NOT MATERIALIZED")) |_| {
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 5 }, read_ast.cte_items[0].column_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.cte_items[0].column_name_tokens.?);
                        try std.testing.expectEqual(@as(usize, 1), read_ast.cte_items[0].column_names.count);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.cte_items[0].column_names.items[0]);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 8 }, read_ast.cte_items[0].materialization_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlCteMaterialization.not_materialized, read_ast.cte_items[0].materialization.?);
                    }
                    try std.testing.expect(read_ast.projection_tokens != null);
                } else if (case.generated == .set_operation) {
                    try std.testing.expect(read_ast.set_operation_tokens != null);
                }
            },
            else => return error.TestUnexpectedResult,
        }
        switch (parsed.statement) {
            .read => |statement| try std.testing.expectEqual(case.read, statement.kind),
            else => return error.TestUnexpectedResult,
        }
    }

    var generated_distinct_on = try ParsedSql.initAlloc(alloc, "SELECT DISTINCT ON (organization_id) organization_id, id FROM usage_records ORDER BY organization_id ASC, created_at DESC");
    defer generated_distinct_on.deinit(alloc);
    try std.testing.expect(generated_distinct_on.generated_statement != null);
    try std.testing.expectEqual(classifier.SqlReadStatementKind.query, generated_distinct_on.readStatementKind().?);
}

test "sql adapter parsed sql retains generated graph nodes as DDL until graph cutover" {
    const alloc = std.testing.allocator;

    const cases = [_]struct {
        sql: []const u8,
        generated: generated_parser.GeneratedSqlGraphKind,
    }{
        .{ .sql = "CREATE GRAPH INDEX docs_edge_graph ON doc_edges", .generated = .create_index },
        .{ .sql = "CREATE GRAPH METRIC docs_pagerank ON doc_edges WITH (metric = 'pagerank')", .generated = .create_metric },
    };

    for (cases) |case| {
        var parsed = try ParsedSql.initAlloc(alloc, case.sql);
        defer parsed.deinit(alloc);
        try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.graph, parsed.generatedStatementKind().?);
        switch (parsed.generated_statement.?.statement) {
            .graph => |kind| try std.testing.expectEqual(case.generated, kind),
            else => return error.TestUnexpectedResult,
        }
        switch (parsed.generated_statement.?.ast.?) {
            .graph => |graph_ast| try std.testing.expectEqual(case.generated, graph_ast.kind),
            else => return error.TestUnexpectedResult,
        }
        switch (parsed.statement) {
            .ddl => {},
            else => return error.TestUnexpectedResult,
        }
    }
}
