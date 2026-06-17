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

const token_mod = @import("token.zig");

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

    pub fn peekKind(self: Cursor, kind: TokenKind) bool {
        return self.pos.* < self.tokens.len and self.tokens[self.pos.*].kind == kind;
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

test "sql adapter parser cursor tracks shared token position" {
    var pos: usize = 0;
    const tokens = [_]Token{
        .{ .kind = .identifier, .text = "select", .source_start = 0, .source_end = 6 },
        .{ .kind = .star, .text = "*", .source_start = 7, .source_end = 8 },
        .{ .kind = .identifier, .text = "from", .source_start = 9, .source_end = 13 },
    };
    const cursor = Cursor.init(tokens[0..], &pos);

    try cursor.expectKeyword("select");
    try std.testing.expectEqual(@as(usize, 1), pos);
    const checkpoint = cursor.checkpoint();
    try std.testing.expect(cursor.matchToken(.star) != null);
    try std.testing.expect(cursor.peekKeyword("from"));
    cursor.restore(checkpoint);
    try std.testing.expect(cursor.peekKind(.star));
    try cursor.advance(2);
    try std.testing.expect(cursor.atEnd());
}
