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
const token_mod = @import("token.zig");

pub const SqlExplainPrefix = ast.SqlExplainPrefix;
pub const Token = token_mod.Token;
pub const TokenKind = token_mod.TokenKind;

pub const Cursor = struct {
    tokens: []const Token,
    pos: *usize,

    pub fn init(tokens: []const Token, pos: *usize) Cursor {
        return .{ .tokens = tokens, .pos = pos };
    }

    pub fn checkpoint(self: Cursor) usize {
        return self.pos.*;
    }

    pub fn restore(self: Cursor, value: usize) void {
        self.pos.* = value;
    }

    pub fn advance(self: Cursor, count: usize) !void {
        if (count > self.tokens.len -| self.pos.*) return error.UnsupportedSqlShape;
        self.pos.* += count;
    }

    pub fn expectKeyword(self: Cursor, keyword: []const u8) !void {
        if (!self.matchKeyword(keyword)) return error.UnsupportedSqlShape;
    }

    pub fn expectToken(self: Cursor, kind: TokenKind) !void {
        if (self.matchToken(kind) == null) return error.UnsupportedSqlShape;
    }

    pub fn matchKeyword(self: Cursor, keyword: []const u8) bool {
        if (self.pos.* >= self.tokens.len) return false;
        const token = self.tokens[self.pos.*];
        if (token.kind != .identifier) return false;
        if (!std.ascii.eqlIgnoreCase(token.text, keyword)) return false;
        self.pos.* += 1;
        return true;
    }

    pub fn matchIdentifierIf(self: Cursor, comptime predicate: fn ([]const u8) bool) ?Token {
        if (self.pos.* >= self.tokens.len) return null;
        const token = self.tokens[self.pos.*];
        if (token.kind != .identifier or !predicate(token.text)) return null;
        self.pos.* += 1;
        return token;
    }

    pub fn matchToken(self: Cursor, kind: TokenKind) ?Token {
        if (self.pos.* >= self.tokens.len) return null;
        const token = self.tokens[self.pos.*];
        if (token.kind != kind) return null;
        self.pos.* += 1;
        return token;
    }

    pub fn peekKeyword(self: Cursor, keyword: []const u8) bool {
        if (self.pos.* >= self.tokens.len) return false;
        const token = self.tokens[self.pos.*];
        return token.kind == .identifier and std.ascii.eqlIgnoreCase(token.text, keyword);
    }

    pub fn peekIdentifierIf(self: Cursor, comptime predicate: fn ([]const u8) bool) bool {
        return self.tokenAtIdentifierIf(self.pos.*, predicate);
    }

    pub fn peekKind(self: Cursor, kind: TokenKind) bool {
        return self.pos.* < self.tokens.len and self.tokens[self.pos.*].kind == kind;
    }

    pub fn tokenAtIdentifierIf(self: Cursor, index: usize, comptime predicate: fn ([]const u8) bool) bool {
        if (index >= self.tokens.len) return false;
        const token = self.tokens[index];
        return token.kind == .identifier and predicate(token.text);
    }

    pub fn functionCallStartsAt(self: Cursor, index: usize, keyword: []const u8) bool {
        if (index + 1 >= self.tokens.len) return false;
        const token = self.tokens[index];
        return token.kind == .identifier and
            std.ascii.eqlIgnoreCase(token.text, keyword) and
            self.tokens[index + 1].kind == .lparen;
    }

    pub fn functionCallStartsAtIf(self: Cursor, index: usize, comptime predicate: fn ([]const u8) bool) bool {
        if (index + 1 >= self.tokens.len) return false;
        const token = self.tokens[index];
        return token.kind == .identifier and
            predicate(token.text) and
            self.tokens[index + 1].kind == .lparen;
    }

    pub fn peekFunctionCall(self: Cursor, keyword: []const u8) bool {
        return self.functionCallStartsAt(self.pos.*, keyword);
    }

    pub fn peekFunctionCallIf(self: Cursor, comptime predicate: fn ([]const u8) bool) bool {
        return self.functionCallStartsAtIf(self.pos.*, predicate);
    }

    pub fn atEnd(self: Cursor) bool {
        return self.pos.* >= self.tokens.len;
    }
};

pub fn expectKeyword(tokens: []const Token, pos: *usize, keyword: []const u8) !void {
    if (!matchKeyword(tokens, pos, keyword)) return error.UnsupportedSqlShape;
}

pub fn expectToken(tokens: []const Token, pos: *usize, kind: TokenKind) !void {
    if (matchToken(tokens, pos, kind) == null) return error.UnsupportedSqlShape;
}

pub fn matchKeyword(tokens: []const Token, pos: *usize, keyword: []const u8) bool {
    if (pos.* >= tokens.len) return false;
    const token = tokens[pos.*];
    if (token.kind != .identifier) return false;
    if (!std.ascii.eqlIgnoreCase(token.text, keyword)) return false;
    pos.* += 1;
    return true;
}

pub fn matchToken(tokens: []const Token, pos: *usize, kind: TokenKind) ?Token {
    if (pos.* >= tokens.len) return null;
    const token = tokens[pos.*];
    if (token.kind != kind) return null;
    pos.* += 1;
    return token;
}

pub fn peekKeyword(tokens: []const Token, pos: usize, keyword: []const u8) bool {
    if (pos >= tokens.len) return false;
    const token = tokens[pos];
    return token.kind == .identifier and std.ascii.eqlIgnoreCase(token.text, keyword);
}

pub fn peekKind(tokens: []const Token, pos: usize, kind: TokenKind) bool {
    return pos < tokens.len and tokens[pos].kind == kind;
}

pub fn atEnd(tokens: []const Token, pos: usize) bool {
    return pos >= tokens.len;
}

pub fn tokensStartWithKeyword(tokens: []const Token, keyword: []const u8) bool {
    return tokens.len > 0 and tokens[0].kind == .identifier and std.ascii.eqlIgnoreCase(tokens[0].text, keyword);
}

pub fn consumeCteMaterializationHint(tokens: []const Token, pos: *usize) !void {
    if (matchKeyword(tokens, pos, "materialized")) return;
    if (matchKeyword(tokens, pos, "not") and !matchKeyword(tokens, pos, "materialized")) {
        return error.UnsupportedSqlShape;
    }
}

pub fn parseExplainPrefix(sql: []const u8) !SqlExplainPrefix {
    var index = skipSqlWhitespace(sql, 0);
    if (!consumeSqlKeyword(sql, &index, "explain")) return error.UnsupportedSqlShape;
    index = skipSqlWhitespace(sql, index);
    if (index >= sql.len) return error.UnsupportedSqlShape;

    var prefix = SqlExplainPrefix{ .inner_sql = "" };
    if (sql[index] == '(') {
        try parseExplainOptions(sql, &index, &prefix);
        index = skipSqlWhitespace(sql, index);
        if (index >= sql.len) return error.UnsupportedSqlShape;
    }

    if (consumeSqlKeyword(sql, &index, "analyze")) {
        prefix.analyze = true;
        index = skipSqlWhitespace(sql, index);
        if (index >= sql.len) return error.UnsupportedSqlShape;
    }

    const inner = std.mem.trim(u8, sql[index..], " \t\r\n;");
    if (inner.len == 0) return error.UnsupportedSqlShape;
    prefix.inner_sql = inner;
    return prefix;
}

fn parseExplainOptions(sql: []const u8, index: *usize, prefix: *SqlExplainPrefix) !void {
    if (index.* >= sql.len or sql[index.*] != '(') return error.UnsupportedSqlShape;
    index.* += 1;
    while (true) {
        index.* = skipSqlWhitespace(sql, index.*);
        if (index.* >= sql.len) return error.UnsupportedSqlShape;
        if (consumeSqlKeyword(sql, index, "format")) {
            index.* = skipSqlWhitespace(sql, index.*);
            if (consumeSqlKeyword(sql, index, "json")) {
                prefix.format = .json;
            } else if (consumeSqlKeyword(sql, index, "text")) {
                prefix.format = .text;
            } else {
                return error.UnsupportedSqlShape;
            }
        } else if (consumeSqlKeyword(sql, index, "verbose")) {
            prefix.verbose = try parseOptionalExplainBool(sql, index, true);
        } else if (consumeSqlKeyword(sql, index, "costs")) {
            prefix.costs = try parseOptionalExplainBool(sql, index, true);
        } else if (consumeSqlKeyword(sql, index, "analyze")) {
            prefix.analyze = try parseOptionalExplainBool(sql, index, true);
        } else {
            return error.UnsupportedSqlShape;
        }

        index.* = skipSqlWhitespace(sql, index.*);
        if (index.* >= sql.len) return error.UnsupportedSqlShape;
        if (sql[index.*] == ',') {
            index.* += 1;
            continue;
        }
        if (sql[index.*] == ')') {
            index.* += 1;
            return;
        }
        return error.UnsupportedSqlShape;
    }
}

fn parseOptionalExplainBool(sql: []const u8, index: *usize, default_value: bool) !bool {
    const before = index.*;
    index.* = skipSqlWhitespace(sql, index.*);
    if (consumeSqlKeyword(sql, index, "true") or
        consumeSqlKeyword(sql, index, "on") or
        consumeSqlKeyword(sql, index, "yes"))
    {
        return true;
    }
    if (consumeSqlKeyword(sql, index, "false") or
        consumeSqlKeyword(sql, index, "off") or
        consumeSqlKeyword(sql, index, "no"))
    {
        return false;
    }
    index.* = before;
    return default_value;
}

fn skipSqlWhitespace(sql: []const u8, start: usize) usize {
    var index = start;
    while (index < sql.len and std.ascii.isWhitespace(sql[index])) : (index += 1) {}
    return index;
}

fn consumeSqlKeyword(sql: []const u8, index: *usize, keyword: []const u8) bool {
    const start = index.*;
    const end = start + keyword.len;
    if (end > sql.len) return false;
    if (!std.ascii.eqlIgnoreCase(sql[start..end], keyword)) return false;
    if (end < sql.len and isSqlIdentifierByte(sql[end])) return false;
    if (start > 0 and isSqlIdentifierByte(sql[start - 1])) return false;
    index.* = end;
    return true;
}

fn isSqlIdentifierByte(byte: u8) bool {
    return std.ascii.isAlphanumeric(byte) or byte == '_';
}

pub fn findTopLevelKeyword(tokens: []const Token, keyword: []const u8) ?usize {
    var depth: usize = 0;
    for (tokens, 0..) |token, i| {
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return null;
                depth -= 1;
            },
            .semicolon => if (depth == 0) return null,
            .identifier => if (depth == 0 and std.ascii.eqlIgnoreCase(token.text, keyword)) return i,
            else => {},
        }
    }
    return null;
}

pub fn findMatchingRParenIndex(tokens: []const Token, lparen_index: usize) ?usize {
    if (lparen_index >= tokens.len or tokens[lparen_index].kind != .lparen) return null;
    var depth: usize = 1;
    var index = lparen_index + 1;
    while (index < tokens.len) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen => depth += 1,
            .rparen => {
                depth -= 1;
                if (depth == 0) return index;
            },
            else => {},
        }
    }
    return null;
}

pub fn stripBalancedOuterParens(raw_tokens: []const Token) []const Token {
    var tokens = raw_tokens;
    while (tokens.len >= 2 and tokens[0].kind == .lparen and tokens[tokens.len - 1].kind == .rparen) {
        var depth: usize = 0;
        var closes_at_end = false;
        for (tokens, 0..) |token, idx| {
            switch (token.kind) {
                .lparen => depth += 1,
                .rparen => {
                    if (depth == 0) return tokens;
                    depth -= 1;
                    if (depth == 0) {
                        closes_at_end = idx == tokens.len - 1;
                        break;
                    }
                },
                else => {},
            }
        }
        if (!closes_at_end) return tokens;
        tokens = tokens[1 .. tokens.len - 1];
    }
    return tokens;
}

pub fn parseWrappedIdentifierOperand(tokens: []const Token, idx: *usize) !Token {
    var wrapped: usize = 0;
    while (idx.* < tokens.len and tokens[idx.*].kind == .lparen) {
        wrapped += 1;
        idx.* += 1;
    }
    if (idx.* >= tokens.len or tokens[idx.*].kind != .identifier) return error.UnsupportedSqlShape;
    const field_token = tokens[idx.*];
    idx.* += 1;
    while (wrapped > 0) {
        if (idx.* >= tokens.len or tokens[idx.*].kind != .rparen) return error.UnsupportedSqlShape;
        idx.* += 1;
        wrapped -= 1;
    }
    return field_token;
}

pub fn hasTopLevelOrBeforeTail(
    tokens: []const Token,
    start: usize,
    comptime tail_clause_keyword: fn ([]const u8) bool,
) bool {
    var depth: usize = 0;
    var index = start;
    while (index < tokens.len) : (index += 1) {
        const token = tokens[index];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => if (depth > 0) {
                depth -= 1;
            },
            .semicolon => if (depth == 0) return false,
            .identifier => if (depth == 0) {
                if (std.ascii.eqlIgnoreCase(token.text, "or")) return true;
                if (tail_clause_keyword(token.text)) return false;
            },
            else => {},
        }
    }
    return false;
}

pub fn findTopLevelTailIndex(
    tokens: []const Token,
    start: usize,
    comptime tail_clause_keyword: fn ([]const u8) bool,
) usize {
    var depth: usize = 0;
    var index = start;
    while (index < tokens.len) : (index += 1) {
        const token = tokens[index];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => if (depth > 0) {
                depth -= 1;
            },
            .identifier => if (depth == 0 and tail_clause_keyword(token.text)) return index,
            .semicolon => if (depth == 0) return index,
            else => {},
        }
    }
    return tokens.len;
}

test "sql adapter parser cursor tracks shared token position" {
    var pos: usize = 0;
    const tokens = [_]Token{
        .{ .kind = .identifier, .text = "select", .source_start = 0, .source_end = 6 },
        .{ .kind = .star, .text = "*", .source_start = 7, .source_end = 8 },
        .{ .kind = .identifier, .text = "from", .source_start = 9, .source_end = 13 },
        .{ .kind = .identifier, .text = "lower", .source_start = 14, .source_end = 19 },
        .{ .kind = .lparen, .text = "(", .source_start = 19, .source_end = 20 },
    };
    const cursor = Cursor.init(tokens[0..], &pos);

    try cursor.expectKeyword("select");
    try std.testing.expectEqual(@as(usize, 1), pos);
    const checkpoint = cursor.checkpoint();
    try std.testing.expect(cursor.matchToken(.star) != null);
    try std.testing.expect(cursor.peekKeyword("from"));
    cursor.restore(checkpoint);
    try std.testing.expect(cursor.peekKind(.star));
    try cursor.advance(1);
    try std.testing.expect(cursor.peekIdentifierIf(testKeywordIsFrom));
    try std.testing.expect(cursor.matchIdentifierIf(testKeywordIsFrom) != null);
    try std.testing.expect(cursor.peekFunctionCall("lower"));
    try std.testing.expect(cursor.peekFunctionCallIf(testKeywordIsLower));
    try cursor.advance(2);
    try std.testing.expect(cursor.atEnd());
}

test "sql adapter parser consumes shared keyword helpers" {
    const materialized_tokens = [_]Token{
        .{ .kind = .identifier, .text = "materialized" },
        .{ .kind = .lparen, .text = "(" },
    };
    try std.testing.expect(tokensStartWithKeyword(materialized_tokens[0..], "materialized"));
    var materialized_pos: usize = 0;
    try consumeCteMaterializationHint(materialized_tokens[0..], &materialized_pos);
    try std.testing.expectEqual(@as(usize, 1), materialized_pos);

    const not_materialized_tokens = [_]Token{
        .{ .kind = .identifier, .text = "not" },
        .{ .kind = .identifier, .text = "materialized" },
        .{ .kind = .lparen, .text = "(" },
    };
    var not_materialized_pos: usize = 0;
    try consumeCteMaterializationHint(not_materialized_tokens[0..], &not_materialized_pos);
    try std.testing.expectEqual(@as(usize, 2), not_materialized_pos);

    const invalid_tokens = [_]Token{
        .{ .kind = .identifier, .text = "not" },
        .{ .kind = .identifier, .text = "ready" },
    };
    var invalid_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, consumeCteMaterializationHint(invalid_tokens[0..], &invalid_pos));
}

test "sql adapter parser parses explain prefixes and options" {
    const basic = try parseExplainPrefix("EXPLAIN SELECT id FROM usage_records;");
    try std.testing.expect(!basic.analyze);
    try std.testing.expectEqual(ast.SqlExplainFormat.text, basic.format);
    try std.testing.expect(!basic.verbose);
    try std.testing.expect(basic.costs);
    try std.testing.expectEqualStrings("SELECT id FROM usage_records", basic.inner_sql);

    const options = try parseExplainPrefix("EXPLAIN (FORMAT JSON, VERBOSE, COSTS OFF, ANALYZE ON) SELECT id FROM usage_records");
    try std.testing.expect(options.analyze);
    try std.testing.expectEqual(ast.SqlExplainFormat.json, options.format);
    try std.testing.expect(options.verbose);
    try std.testing.expect(!options.costs);
    try std.testing.expectEqualStrings("SELECT id FROM usage_records", options.inner_sql);

    const analyze = try parseExplainPrefix("EXPLAIN ANALYZE INSERT INTO usage_records (id) VALUES ('u1')");
    try std.testing.expect(analyze.analyze);
    try std.testing.expectEqualStrings("INSERT INTO usage_records (id) VALUES ('u1')", analyze.inner_sql);

    try std.testing.expectError(error.UnsupportedSqlShape, parseExplainPrefix("EXPLAIN (FORMAT YAML) SELECT 1"));
    try std.testing.expectError(error.UnsupportedSqlShape, parseExplainPrefix("EXPLAINED SELECT 1"));
    try std.testing.expectError(error.UnsupportedSqlShape, parseExplainPrefix("EXPLAIN"));
}

fn testKeywordIsFrom(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "from");
}

fn testKeywordIsLower(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "lower");
}

test "sql adapter parser strips balanced outer parens and parses wrapped identifiers" {
    const tokens = [_]Token{
        .{ .kind = .lparen, .text = "(", .source_start = 0, .source_end = 1 },
        .{ .kind = .lparen, .text = "(", .source_start = 1, .source_end = 2 },
        .{ .kind = .identifier, .text = "status", .source_start = 2, .source_end = 8 },
        .{ .kind = .rparen, .text = ")", .source_start = 8, .source_end = 9 },
        .{ .kind = .rparen, .text = ")", .source_start = 9, .source_end = 10 },
    };
    const stripped = stripBalancedOuterParens(tokens[0..]);
    try std.testing.expectEqual(@as(usize, 1), stripped.len);
    try std.testing.expectEqualStrings("status", stripped[0].text);

    var idx: usize = 0;
    const wrapped = try parseWrappedIdentifierOperand(tokens[1..4], &idx);
    try std.testing.expectEqualStrings("status", wrapped.text);
    try std.testing.expectEqual(@as(usize, 3), idx);

    const sibling_tokens = [_]Token{
        .{ .kind = .lparen, .text = "(", .source_start = 0, .source_end = 1 },
        .{ .kind = .identifier, .text = "a", .source_start = 1, .source_end = 2 },
        .{ .kind = .rparen, .text = ")", .source_start = 2, .source_end = 3 },
        .{ .kind = .identifier, .text = "and", .source_start = 4, .source_end = 7 },
        .{ .kind = .lparen, .text = "(", .source_start = 8, .source_end = 9 },
        .{ .kind = .identifier, .text = "b", .source_start = 9, .source_end = 10 },
        .{ .kind = .rparen, .text = ")", .source_start = 10, .source_end = 11 },
    };
    try std.testing.expectEqual(@as(usize, sibling_tokens.len), stripBalancedOuterParens(sibling_tokens[0..]).len);
}

test "sql adapter parser detects top-level OR before tail clauses" {
    const tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .eq, .text = "=" },
        .{ .kind = .string, .text = "'active'" },
        .{ .kind = .identifier, .text = "or" },
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .eq, .text = "=" },
        .{ .kind = .string, .text = "'pending'" },
        .{ .kind = .identifier, .text = "order" },
    };
    try std.testing.expect(hasTopLevelOrBeforeTail(tokens[0..], 0, testWhereTailKeyword));

    const nested_tokens = [_]Token{
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "a" },
        .{ .kind = .identifier, .text = "or" },
        .{ .kind = .identifier, .text = "b" },
        .{ .kind = .rparen, .text = ")" },
        .{ .kind = .identifier, .text = "order" },
    };
    try std.testing.expect(!hasTopLevelOrBeforeTail(nested_tokens[0..], 0, testWhereTailKeyword));
}

fn testWhereTailKeyword(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "order");
}

test "sql adapter parser finds top-level tail clause index" {
    const tokens = [_]Token{
        .{ .kind = .identifier, .text = "window" },
        .{ .kind = .identifier, .text = "w" },
        .{ .kind = .identifier, .text = "as" },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "order" },
        .{ .kind = .identifier, .text = "by" },
        .{ .kind = .identifier, .text = "created_at" },
        .{ .kind = .rparen, .text = ")" },
        .{ .kind = .identifier, .text = "order" },
        .{ .kind = .identifier, .text = "by" },
    };
    try std.testing.expectEqual(@as(usize, 8), findTopLevelTailIndex(tokens[0..], 0, testWhereTailKeyword));

    const semicolon_tokens = [_]Token{
        .{ .kind = .identifier, .text = "window" },
        .{ .kind = .identifier, .text = "w" },
        .{ .kind = .identifier, .text = "as" },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "partition" },
        .{ .kind = .identifier, .text = "by" },
        .{ .kind = .identifier, .text = "tenant_id" },
        .{ .kind = .rparen, .text = ")" },
        .{ .kind = .semicolon, .text = ";" },
        .{ .kind = .identifier, .text = "order" },
    };
    try std.testing.expectEqual(@as(usize, 8), findTopLevelTailIndex(semicolon_tokens[0..], 0, testWhereTailKeyword));
}
