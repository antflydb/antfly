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
const lexer = @import("lexer.zig");
const token_mod = @import("token.zig");

pub const Token = token_mod.Token;
pub const TokenKeyword = token_mod.TokenKeyword;
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

    pub fn expectKeywordTag(self: Cursor, keyword: TokenKeyword) !void {
        if (!self.matchKeywordTag(keyword)) return error.UnsupportedSqlShape;
    }

    pub fn expectToken(self: Cursor, kind: TokenKind) !void {
        if (self.matchToken(kind) == null) return error.UnsupportedSqlShape;
    }

    pub fn matchKeyword(self: Cursor, keyword: []const u8) bool {
        if (self.pos.* >= self.tokens.len) return false;
        const token = self.tokens[self.pos.*];
        if (!token.matchesKeyword(keyword)) return false;
        self.pos.* += 1;
        return true;
    }

    pub fn matchKeywordTag(self: Cursor, keyword: TokenKeyword) bool {
        if (self.pos.* >= self.tokens.len) return false;
        if (!self.tokens[self.pos.*].matchesKeywordTag(keyword)) return false;
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

    pub fn matchIdentifierTokenIf(self: Cursor, comptime predicate: fn (Token) bool) ?Token {
        if (self.pos.* >= self.tokens.len) return null;
        const token = self.tokens[self.pos.*];
        if (token.kind != .identifier or !predicate(token)) return null;
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
        return self.tokens[self.pos.*].matchesKeyword(keyword);
    }

    pub fn peekKeywordTag(self: Cursor, keyword: TokenKeyword) bool {
        return self.pos.* < self.tokens.len and self.tokens[self.pos.*].matchesKeywordTag(keyword);
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
        return self.tokens[index].matchesKeyword(keyword) and
            self.tokens[index + 1].kind == .lparen;
    }

    pub fn functionCallStartsAtTag(self: Cursor, index: usize, keyword: TokenKeyword) bool {
        if (index + 1 >= self.tokens.len) return false;
        return self.tokens[index].matchesKeywordTag(keyword) and
            self.tokens[index + 1].kind == .lparen;
    }

    pub fn functionCallStartsAtIf(self: Cursor, index: usize, comptime predicate: fn ([]const u8) bool) bool {
        if (index + 1 >= self.tokens.len) return false;
        const token = self.tokens[index];
        return token.kind == .identifier and
            predicate(token.text) and
            self.tokens[index + 1].kind == .lparen;
    }

    pub fn functionCallStartsAtTokenIf(self: Cursor, index: usize, comptime predicate: fn (Token) bool) bool {
        if (index + 1 >= self.tokens.len) return false;
        const token = self.tokens[index];
        return token.kind == .identifier and
            predicate(token) and
            self.tokens[index + 1].kind == .lparen;
    }

    pub fn peekFunctionCall(self: Cursor, keyword: []const u8) bool {
        return self.functionCallStartsAt(self.pos.*, keyword);
    }

    pub fn peekFunctionCallTag(self: Cursor, keyword: TokenKeyword) bool {
        return self.functionCallStartsAtTag(self.pos.*, keyword);
    }

    pub fn peekFunctionCallIf(self: Cursor, comptime predicate: fn ([]const u8) bool) bool {
        return self.functionCallStartsAtIf(self.pos.*, predicate);
    }

    pub fn peekFunctionCallTokenIf(self: Cursor, comptime predicate: fn (Token) bool) bool {
        return self.functionCallStartsAtTokenIf(self.pos.*, predicate);
    }

    pub fn atEnd(self: Cursor) bool {
        return self.pos.* >= self.tokens.len;
    }
};

pub fn expectKeyword(tokens: []const Token, pos: *usize, keyword: []const u8) !void {
    if (!matchKeyword(tokens, pos, keyword)) return error.UnsupportedSqlShape;
}

pub fn expectKeywordTag(tokens: []const Token, pos: *usize, keyword: TokenKeyword) !void {
    if (!matchKeywordTag(tokens, pos, keyword)) return error.UnsupportedSqlShape;
}

pub fn expectToken(tokens: []const Token, pos: *usize, kind: TokenKind) !void {
    if (matchToken(tokens, pos, kind) == null) return error.UnsupportedSqlShape;
}

pub fn matchKeyword(tokens: []const Token, pos: *usize, keyword: []const u8) bool {
    if (pos.* >= tokens.len) return false;
    if (!tokens[pos.*].matchesKeyword(keyword)) return false;
    pos.* += 1;
    return true;
}

pub fn matchKeywordTag(tokens: []const Token, pos: *usize, keyword: TokenKeyword) bool {
    if (pos.* >= tokens.len) return false;
    if (!tokens[pos.*].matchesKeywordTag(keyword)) return false;
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
    return tokens[pos].matchesKeyword(keyword);
}

pub fn peekKeywordTag(tokens: []const Token, pos: usize, keyword: TokenKeyword) bool {
    return pos < tokens.len and tokens[pos].matchesKeywordTag(keyword);
}

pub fn peekKind(tokens: []const Token, pos: usize, kind: TokenKind) bool {
    return pos < tokens.len and tokens[pos].kind == kind;
}

pub fn atEnd(tokens: []const Token, pos: usize) bool {
    return pos >= tokens.len;
}

pub fn tokensStartWithKeyword(tokens: []const Token, keyword: []const u8) bool {
    return tokens.len > 0 and tokens[0].matchesKeyword(keyword);
}

pub fn tokensStartWithKeywordTag(tokens: []const Token, keyword: TokenKeyword) bool {
    return tokens.len > 0 and tokens[0].matchesKeywordTag(keyword);
}

pub fn consumeCteMaterializationHint(tokens: []const Token, pos: *usize) !void {
    if (matchKeywordTag(tokens, pos, .materialized)) return;
    if (matchKeywordTag(tokens, pos, .not) and !matchKeywordTag(tokens, pos, .materialized)) {
        return error.UnsupportedSqlShape;
    }
}

pub fn findTopLevelKeyword(tokens: []const Token, keyword: []const u8) ?usize {
    return findTopLevelKeywordFromIndex(tokens, 0, keyword);
}

pub fn findTopLevelKeywordFromIndex(tokens: []const Token, start: usize, keyword: []const u8) ?usize {
    if (token_mod.keywordFromIdentifier(keyword)) |keyword_tag| {
        return findTopLevelKeywordTagFromIndex(tokens, start, keyword_tag);
    }
    var depth: usize = 0;
    var i = start;
    while (i < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return null;
                depth -= 1;
            },
            .semicolon => if (depth == 0) return null,
            .identifier => if (depth == 0 and token.matchesKeyword(keyword)) return i,
            else => {},
        }
    }
    return null;
}

pub fn findTopLevelKeywordTag(tokens: []const Token, keyword: TokenKeyword) ?usize {
    return findTopLevelKeywordTagFromIndex(tokens, 0, keyword);
}

pub fn findTopLevelKeywordTagFromIndex(tokens: []const Token, start: usize, keyword: TokenKeyword) ?usize {
    var depth: usize = 0;
    var i = start;
    while (i < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return null;
                depth -= 1;
            },
            .semicolon => if (depth == 0) return null,
            .identifier => if (depth == 0 and token.matchesKeywordTag(keyword)) return i,
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

pub fn findMatchingRParenAfterOpenIndex(tokens: []const Token, pos_after_lparen: usize) ?usize {
    if (pos_after_lparen == 0) return null;
    return findMatchingRParenIndex(tokens, pos_after_lparen - 1);
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
                if (token.matchesKeywordTag(.@"or")) return true;
                if (tail_clause_keyword(token.text)) return false;
            },
            else => {},
        }
    }
    return false;
}

pub fn hasTopLevelOrBeforeTailToken(
    tokens: []const Token,
    start: usize,
    comptime tail_clause_keyword: fn (Token) bool,
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
                if (token.matchesKeywordTag(.@"or")) return true;
                if (tail_clause_keyword(token)) return false;
            },
            else => {},
        }
    }
    return false;
}

pub fn hasTopLevelOrBeforeCloseParen(tokens: []const Token, start: usize) bool {
    var depth: usize = 0;
    var index = start;
    while (index < tokens.len) : (index += 1) {
        const token = tokens[index];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return false;
                depth -= 1;
            },
            .identifier => if (depth == 0 and token.matchesKeywordTag(.@"or")) return true,
            else => {},
        }
    }
    return false;
}

pub fn predicateStartIndexAfterOpenParens(tokens: []const Token, index: usize) usize {
    var out = index;
    while (out < tokens.len and tokens[out].kind == .lparen) : (out += 1) {}
    return out;
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

pub fn findTopLevelTailIndexToken(
    tokens: []const Token,
    start: usize,
    comptime tail_clause_keyword: fn (Token) bool,
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
            .identifier => if (depth == 0 and tail_clause_keyword(token)) return index,
            .semicolon => if (depth == 0) return index,
            else => {},
        }
    }
    return tokens.len;
}

test "sql adapter parser cursor tracks shared token position" {
    var pos: usize = 0;
    var tokens = try lexer.tokenizeAlloc(std.testing.allocator, "select * from lower(");
    defer lexer.freeTokens(std.testing.allocator, &tokens);
    const cursor = Cursor.init(tokens.items, &pos);

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
    try std.testing.expect(cursor.peekFunctionCallTag(.lower));
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
    try std.testing.expect(tokensStartWithKeywordTag(materialized_tokens[0..], .materialized));
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
    try std.testing.expectEqual(@as(usize, 4), findMatchingRParenAfterOpenIndex(tokens[0..], 1).?);

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
    try std.testing.expect(hasTopLevelOrBeforeTailToken(tokens[0..], 0, testWhereTailKeywordToken));

    const nested_tokens = [_]Token{
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "a" },
        .{ .kind = .identifier, .text = "or" },
        .{ .kind = .identifier, .text = "b" },
        .{ .kind = .rparen, .text = ")" },
        .{ .kind = .identifier, .text = "order" },
    };
    try std.testing.expect(!hasTopLevelOrBeforeTailToken(nested_tokens[0..], 0, testWhereTailKeywordToken));

    const before_close_tokens = [_]Token{
        .{ .kind = .identifier, .text = "a" },
        .{ .kind = .identifier, .text = "or" },
        .{ .kind = .identifier, .text = "b" },
        .{ .kind = .rparen, .text = ")" },
    };
    try std.testing.expect(hasTopLevelOrBeforeCloseParen(before_close_tokens[0..], 0));

    const after_close_tokens = [_]Token{
        .{ .kind = .identifier, .text = "a" },
        .{ .kind = .rparen, .text = ")" },
        .{ .kind = .identifier, .text = "or" },
        .{ .kind = .identifier, .text = "b" },
    };
    try std.testing.expect(!hasTopLevelOrBeforeCloseParen(after_close_tokens[0..], 0));

    const wrapped_tokens = [_]Token{
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "status" },
    };
    try std.testing.expectEqual(@as(usize, 2), predicateStartIndexAfterOpenParens(wrapped_tokens[0..], 0));
    try std.testing.expectEqual(@as(usize, 2), predicateStartIndexAfterOpenParens(wrapped_tokens[0..], 2));
}

fn testWhereTailKeywordToken(token: Token) bool {
    return token.matchesKeywordTag(.order);
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
    try std.testing.expectEqual(@as(usize, 8), findTopLevelTailIndexToken(tokens[0..], 0, testWhereTailKeywordToken));

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
    try std.testing.expectEqual(@as(usize, 8), findTopLevelTailIndexToken(semicolon_tokens[0..], 0, testWhereTailKeywordToken));
}
